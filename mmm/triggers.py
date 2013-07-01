import bson
from collections import defaultdict
import gevent
import logging
import threading
import time

log = logging.getLogger(__name__)

class Triggers(object):
  """
  last_op_message_read_at: time in seconds, only op log messages that happened after this time
  will be read.
  """
  def __init__(self, source_id, oplog, checkpoint_collection):
    self.source_id = source_id
    self._oplog = oplog
    self._checkpoint = checkpoint_collection
    self._oplog.ensure_index('ts')
    self._callbacks = defaultdict(list)
    self.query_id = {"_id": self.source_id}
    self.stop_event = threading.Event()

  def stop(self):
    self.stop_event.set()

  def run(self):
    last_op_message_read_at = self._set_and_get_checkpoint()
    log.debug("Reading oplog messages after %s", last_op_message_read_at)
    while not self.stop_event.isSet():
      last_op_message_read_at = self._tail_oplog(last_op_message_read_at)

  def _tail_oplog(self, checkpoint):
    spec = {"ts": {'$gt': checkpoint}}
    q = self._oplog.find(spec, tailable=True, await_data=True)
    for op_doc in q.sort('$natural'):
      for (namespace, op), callbacks in self._callbacks.items():
        if op == op_doc['op'] and namespace in [op_doc['ns'], op_doc['ns'].split(".", 1)[0] + ".*"]:
          for callback in callbacks:
            callback(**op_doc)
      checkpoint = op_doc['ts']
      self._checkpoint.update(self.query_id, {'$set': {'checkpoint': checkpoint}})
    return checkpoint

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