import bson
from collections import defaultdict
from functools import wraps
import gevent
import logging
from pymongo import Connection
from pymongo.errors import AutoReconnect, OperationFailure
import sys
import time

log = logging.getLogger(__name__)

RECONNECT_SLEEP_TIME = 60
IDLE_SLEEP_TIME = 1
LOG_COUNT = 1000

def log_counts(func):
  @wraps(func)
  def f(*args, **kwargs):
    try:
      value = func(*args, **kwargs)
      f.calls += 1
      if f.calls % LOG_COUNT == 0:
        log.info("Replicated %s documents thus far", f.calls)
      return value
    except:
      raise # not responsible for dealing with this exception

  f.calls = 0
  return f

class Triggers(object):
  """
  last_op_message_read_at: time in seconds, only op log messages that happened after this time
  will be read.
  """
  def __init__(self, source_id, source_uri, *connection_args, **connection_kwargs):
    self.source_id = source_id
    self.source_uri = source_uri
    self.connection_args = connection_args
    self.connection_kwargs = connection_kwargs
    self._callbacks = defaultdict(list)
    self.query_id = {"_id": self.source_id}
    self.stop_event = gevent.event.Event()
    self._oplog = None
    self._checkpoint = None

  def stop(self):
    self.stop_event.set()

  def connect(self):
    connection = Connection(self.source_uri, *self.connection_args, **self.connection_kwargs)
    self._oplog = connection.local.oplog.rs
    self._checkpoint = connection.local.mmm

  def run(self):
    self.connect()
    last_op_message_read_at = self._set_and_get_checkpoint()
    log.debug("Reading oplog messages after %s", last_op_message_read_at)
    while not self.stop_event.isSet():
      try:
        self._tail_oplog(last_op_message_read_at)
      except (AutoReconnect, OperationFailure):
        log.warn("Connection to master failed at %s, sleeping for %s seconds before attempting to re-connect...", (self.source_uri, RECONNECT_SLEEP_TIME))
        gevent.sleep(RECONNECT_SLEEP_TIME)
        try:
          self.connect()
        except Exception:
          log.error("Unable to reconnect to master at %s, exiting", self.source_uri, exc_info=1)
          sys.exit(0)

  def _tail_oplog(self, checkpoint):
    spec = {"ts": {'$gt': checkpoint}}
    cursor = self._oplog.find(spec, tailable=True, await_data=True)
    try:
      while cursor.alive:
        try:
          op_doc = cursor.next()
          self._exec_callbacks(op_doc)
          checkpoint = op_doc['ts']
          self._checkpoint.update(self.query_id, {'$set': {'checkpoint': checkpoint}})
          log.debug("advancing checkpoint to %s", checkpoint)
        except StopIteration:
          gevent.sleep(IDLE_SLEEP_TIME)
    finally:
      cursor.close()

  @log_counts
  def _exec_callbacks(self, op_doc):
    for callback in self._callbacks.get((op_doc['ns'], op_doc['op']), []):
      callback(**op_doc)

  def _set_and_get_checkpoint(self):
    #is there a command in Mongo to do this in one shot: if the document doesn't exist just create it?
    checkpoint_document = self._checkpoint.find_one(self.query_id) or {}
    if not checkpoint_document:
      self._checkpoint.save(self.query_id)
    return checkpoint_document.get("checkpoint", bson.Timestamp(long(time.time()), 0))

  def register(self, namespace, operations, callback_func):
    """
    namespace is the database and collection pair describing the operation.
    e.g. a value of "foo.bar" is an operation in the "foo" database, "bar" collection.

    opertions is a string of all the op log operations to listen for:
     i: insert
     u: update
     d: delete
    e.g. a value of "iu" would only trigger inserts and updates to be replciated.
    """
    for op in operations:
      self._callbacks[(namespace, op)].append(callback_func)
