from functools import wraps
import gevent
import logging
from pymongo import Connection
from pymongo.errors import AutoReconnect, OperationFailure

from mmm.triggers import Triggers

log = logging.getLogger(__name__)

RECONNECT_SLEEP_TIME = 60

class ReplicationEngine(object):

  def __init__(self, source_id, source_uri, destinations, *connection_args, **connection_kwargs):
    self._connection = Connection(source_uri, *connection_args, **connection_kwargs)
    self._collection = self._connection.local.mmm
    self.triggers = Triggers(source_id, source_uri, *connection_args, **connection_kwargs)
    for dest in destinations:
      for namespace in dest["namespaces"]:
        replicator = Replicator(source_id, dest["id"], dest["uri"], *namespace["dest"].split(".", 1))
        self.triggers.register(namespace["source"], dest.get("operations", "iud"), replicator)

  def start(self, checkpoint=None):
    gevent.spawn_link_exception(self.triggers.run)


def reconnect_on_error(func):
  @wraps(func)
  def f(self, *args, **kwargs):
    try:
      func(self, *args, **kwargs)
    except (AutoReconnect, OperationFailure):
      log.warn("Connection to master failed at %s, sleeping for %s seconds before attempting to re-connect and retry...", (self.destination_uri, RECONNECT_SLEEP_TIME))
      gevent.sleep(RECONNECT_SLEEP_TIME)
      try:
        self.connect()
      except Exception:
        log.error("Unable to reconnect to master at %s, exiting", self.destination_uri, exc_info=1)
        sys.exit(0)
      else:
        func(self, *args, **kwargs)
  return f

MMM_REPL_FLAG = '__mmm'
class Replicator(object):

    def __init__(self, source_id, destination_id, destination_uri, destination_database, destination_collection):
        self.source_id = source_id
        self.destination_id = destination_id
        self.destination_uri = destination_uri
        self.destination_database = destination_database
        self.destination_collection = destination_collection
        self.connect()

    def connect(self):
        self._connection = Connection(self.destination_uri)
        self._collection = self._connection[self.destination_database][self.destination_collection]

    def __call__(self, *args, **kwargs):
      return self.replicate(*args, **kwargs)

    @reconnect_on_error
    def replicate(self, ts, h, op, ns, o, o2=None, b=False, v=None):
        log.debug('%s <= %s: %s %s %s', self.destination_id, self.source_id, op, ns, o)
        if op == 'i':
            self.insert(o)
        elif op == 'u':
            self.update(o2, o, b)
        elif op == 'd':
            self.delete(o)

    def insert(self, document):
        if document.get(MMM_REPL_FLAG) == self.destination_id:
            log.debug('%s: skip', self.destination_id)
            return
        document.setdefault(MMM_REPL_FLAG, self.source_id)
        self._collection.insert(document)

    def update(self, query_for_document, updated_document, is_upsert):
        if any(k.startswith('$') for k in updated_document):
            # With modifiers, check & update setters
            setters = updated_document.setdefault('$set', {})
        else:
            # Without modifiers, check & update the doc directly
            setters = updated_document
        if setters.get(MMM_REPL_FLAG) == self.destination_id:
            log.debug('%s: skip', self.destination_id)
            return
        setters.setdefault(MMM_REPL_FLAG, self.source_id)

        self._collection.update(query_for_document, updated_document, is_upsert)

    def delete(self, document):
        self._collection.remove(document)
