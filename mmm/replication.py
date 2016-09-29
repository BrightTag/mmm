from bson.json_util import dumps
from collections import defaultdict
from flatdict import FlatDict
from functools import wraps
import gevent
import hashlib
import json
import logging
from pymongo import Connection
from pymongo.errors import AutoReconnect, OperationFailure
import sys
import time

from mmm.triggers import Triggers

log = logging.getLogger(__name__)

RECONNECT_SLEEP_TIME = 60

class ReplicationEngine(object):

  def __init__(self, source_id, source_uri, destinations, *connection_args, **connection_kwargs):
    self._connection = Connection(source_uri, *connection_args, **connection_kwargs)
    self._collection = self._connection.local.mmm
    self.triggers = Triggers(source_id, source_uri, *connection_args, **connection_kwargs)

    aggregate_replicators = {}
    for db_collection in ReplicationEngine.get_replicated_collections(destinations):
      namespace = ".".join(db_collection)
      aggregate_replicator = AggregateReplicator(source_id, source_uri, db_collection[0], db_collection[1])
      aggregate_replicators[namespace] = aggregate_replicator
      self.triggers.register(namespace, "iud", aggregate_replicator)

    for dest in destinations:
      for namespace in dest["namespaces"]:
        source = namespace["source"]
        replicator = Replicator(source_id, dest["id"], dest["uri"], *namespace["dest"].split(".", 1))
        aggregate_replicators[source].register(replicator, namespace["source"], dest.get("operations", "iud"))

  def start(self, checkpoint=None):
    gevent.spawn_link_exception(self.triggers.run)

  @staticmethod
  def get_replicated_collections(destinations):
    """Determines the unique set of collections this MMM is configured to replicate
    :param destinations: Dictionary of replication destination namespaces
    :return: A set of tuples representing the sources replicated in the format (db_name, collection_name)
    """
    return set([item for sublist in
      [[tuple(ns['source'].split('.', 1))
        for ns in destination["namespaces"]]
          for destination in destinations]
            for item in sublist])


def reconnect_on_error(func):
  @wraps(func)
  def f(self, *args, **kwargs):
    try:
      func(self, *args, **kwargs)
    except (AutoReconnect, OperationFailure):
      log.warn("Connection to master failed at %s, sleeping for %s seconds before attempting to re-connect and retry...", self.destination_uri, RECONNECT_SLEEP_TIME)
      gevent.sleep(RECONNECT_SLEEP_TIME)
      try:
        self.connect()
      except Exception:
        log.error("Unable to reconnect to master at %s, exiting", self.destination_uri, exc_info=1)
        sys.exit(0)
      else:
        func(self, *args, **kwargs)
  return f

def ordered(obj):
  if isinstance(obj, dict):
    return sorted((k, ordered(v)) for k, v in obj.items())
  if isinstance(obj, list):
    return [ordered(x) for x in obj]
  else:
    return obj

MMM_METADATA = '__mmm'
MMM_TIMESTAMP = "source_ts"
MMM_HASH = "hash"
MMM_SKIP_OP = "__mmm_skip"

class Replicator(object):
  """
  Replicates insert/update/delete operations occurring locally to a single remote destination db/collection
  """
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
  def replicate(self, op, ns, o, o2=None, b=False):
    log.debug('%s <= %s: %s %s %s', self.destination_id, self.source_id, op, ns, o)
    if op == 'i':
      self.insert(o)
    elif op == 'u':
      self.update(o2, o, b)
    elif op == 'd':
      self.delete(o)

  def insert(self, document):
    document[MMM_METADATA][self.destination_id] = document[MMM_METADATA][MMM_TIMESTAMP]
    self._collection.insert(document)

  def update(self, query_for_document, updated_document, is_upsert):
    if any(k.startswith('$') for k in updated_document):
      # With modifiers, check & update setters
      setters = updated_document.setdefault('$set', {})
      if MMM_METADATA in setters:
        setters[MMM_METADATA][self.destination_id] = setters[MMM_METADATA][MMM_TIMESTAMP]
    else:
      # Without modifiers, check & update the doc directly
      updated_document[MMM_METADATA][self.destination_id] = updated_document[MMM_METADATA][MMM_TIMESTAMP]

    self._collection.update(query_for_document, updated_document, is_upsert)

  def delete(self, document):
    self._collection.remove(document)

class AggregateReplicator(object):
  """
  Handles replication of insert/update/delete operations to multiple destinations for a single collection
  """

  def __init__(self, source_id, uri, database, collection):
    log.info("Creating aggregate replicator for %s.%s", database, collection)
    self.source_id = source_id
    self.uri = uri
    self.database = database
    self.collection = collection
    self._replicators = defaultdict(list)
    self.connect()

  def connect(self):
    self._connection = Connection(self.uri)
    self._collection = self._connection[self.database][self.collection]

  def __call__(self, *args, **kwargs):
    return self.replicate(*args, **kwargs)

  @reconnect_on_error
  def replicate(self, ts, h, op, ns, o, o2=None, b=False, v=None):
    log.debug('Aggregate replicator processing: %s: %s %s %s, ts: %s', self.source_id, op, ns, o, ts)
    if o.get(MMM_SKIP_OP, False):
      log.debug("skipping internal operation")
      return
    if op == 'i':
      if AggregateReplicator.is_local_replication(o):
        self.ack_replication(o[MMM_METADATA], {"_id": o["_id"]}, ns)
      else:
        self.replicate_local_write(o, {"_id": o["_id"]}, op, ns)
    elif op == 'u':
      is_set_query = "$set" in o
      if MMM_METADATA in o and type(o[MMM_METADATA]) is not dict:
        # old record - make sure we overwrite the metadata completely
        del o[MMM_METADATA]
      if AggregateReplicator.is_local_replication(o, is_set_query):
        self.ack_replication((o["$set"][MMM_METADATA] if is_set_query else o[MMM_METADATA]), o2, ns)
      elif not AggregateReplicator.is_remote_metadata_update(o):
        self.replicate_local_write(o, o2, op, ns, is_set_query)
    elif op == 'd':
      self.replicate_all(op, ns, o, o2, b)

  def replicate_all(self, op, ns, o, o2, b=False):
    for replicator in self._replicators.get((ns, op), []):
      replicator.replicate(op, ns, o, o2, b)

  def register(self, replicator, namespace, operations):
    for op in operations:
      self._replicators[(namespace, op)].append(replicator)

  def ack_replication(self, metadata, object_id, ns):
    """
    Sends an acknowledgement of successful replication to all destinations
    :param metadata: dict representation of the MMM metadata field for the modified object
    :param object_id: ID of the modified object
    :param ns: namespace
    """
    if self.source_id in metadata:
      timestamp = metadata[MMM_TIMESTAMP]
      self.replicate_all("u", ns, {"$set": {MMM_METADATA + "." + self.source_id: timestamp}}, object_id)

  def replicate_local_write(self, o, object_id, op, ns, is_set_query=False):
    """
    Replicates a local insert or update to all other nodes
    :param o: The object passed to a mongo insert/update query
    :param object_id: ID of the modified object
    :param op: The operation ("i" or "u")
    :param ns: namespace
    :param is_set_query: True if this query used the $set operator
    """
    timestamp = AggregateReplicator.timestamp()
    metadata = {
        "source": self.source_id,
         MMM_TIMESTAMP: timestamp,
         self.source_id: timestamp,
         MMM_HASH: AggregateReplicator.hash(o)
      }
    if is_set_query:
      o["$set"][MMM_METADATA] = metadata
    else:
      o[MMM_METADATA] = metadata
    self._collection.update(object_id, o)
    self.replicate_all(op, ns, o, object_id)

  @staticmethod
  def timestamp():
    """
    :return: The number of milliseconds since the unix epoch
    """
    return long(time.time() * 1000)

  @staticmethod
  def is_remote_metadata_update(o):
    """
    :param o: The object passed to a mongo update query
    :return: True if the object represents an update of MMM metadata
    """
    return "$set" in o and any(k.startswith(MMM_METADATA + ".") for k in o["$set"])

  @staticmethod
  def is_local_replication(o, is_set_query=False):
    """
    :param o: The object passed to a mongo insert/update query
    :param is_set_query: True if the query uses the $set operation
    :return: True if a query is a local replication of an update originating at another master node
    """
    metadata_in_doc = MMM_METADATA in o
    if metadata_in_doc and MMM_HASH in o[MMM_METADATA]:
      # The hash is used to discriminate between application based updates
      # that have MMM metadata present, and replications by this process.
      # Application-based updates won't have hashes that match the updated document
      return AggregateReplicator.hash(o) == o[MMM_METADATA][MMM_HASH]
    return metadata_in_doc or (is_set_query and MMM_METADATA in o["$set"])

  @staticmethod
  def hash(to_hash):
    """
    :param to_hash: dictionary to generate a hash code for
    :return: a hash code for the dictionary, excluding the MMM_METADATA field
    """
    # Don't include the MMM_METADATA field in the hash, copy to avoid deleting it in the
    # original dict
    without_mmm = dict([(k, v) for k, v in to_hash.iteritems()
      if k != MMM_METADATA])
    return hashlib.md5(dumps(ordered(without_mmm))).hexdigest()