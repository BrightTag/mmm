
from unittest import TestCase
from mock import MagicMock
import bson
import time
from mmm.triggers import Triggers

class TriggersTailTest(TestCase):

  def setUp(self):
    self.cursor = MagicMock()
    self.oplog = MagicMock()
    self.oplog.find.return_value = self.cursor
    self.checkpoint_col = MagicMock()
    self.callback_func = MagicMock()
    self.trigger = Triggers("my-source-id", self.oplog, self.checkpoint_col)
    self.default_checkpoint = 0L

  def _assert_calls(self, checkpoint):
    self.oplog.ensure_index.assert_called_with('ts')
    self.oplog.find.assert_called_with({"ts": {'$gt': checkpoint}}, tailable=True, await_data=True)
    self.cursor.sort.assert_called_with('$natural')

  def test_tail_with_no_messages(self):
    self.cursor.sort.return_value = []
    self.trigger.register("foodb.barcol", "i", self.callback_func)

    new_checkpoint = self.trigger._tail_oplog(self.default_checkpoint)

    self._assert_calls(self.default_checkpoint)
    self.assertFalse(self.callback_func.called)
    self.assertEquals(self.default_checkpoint, new_checkpoint)

  def test_tail_with_messages(self):
    op_timestamp = bson.Timestamp(long(time.time()), 0)
    #some oplog message I copied from Mongo
    op_message = {
      "ts" : op_timestamp,
      "h" : -2429474310205918006,
      "op" : "u",
      "ns" : "foodb.barcol",
      "o2" : {
        "_id" : "51d2daa81fa97fc9611102cf"
      },
      "o" : {
        "$set" : {
          "bar" : "baz"
        }
      }
    }
    self.cursor.sort.return_value = [op_message]
    self.trigger.register("foodb.barcol", "u", self.callback_func)

    new_checkpoint = self.trigger._tail_oplog(self.default_checkpoint)

    self._assert_calls(self.default_checkpoint)
    self.callback_func.assert_called_with(**op_message)
    self.assertEquals(op_timestamp, new_checkpoint)

  def test_tail_with_nonmatching_namespace(self):
    op_timestamp = bson.Timestamp(long(time.time()), 0)
    op_message = {
      "ts" : op_timestamp,
      "op" : "u",
      "ns" : "foodb.barcol"
    }
    self.cursor.sort.return_value = [op_message]
    self.trigger.register("adifferentdb.adifferentcol", "u", self.callback_func)

    new_checkpoint = self.trigger._tail_oplog(self.default_checkpoint)

    self._assert_calls(self.default_checkpoint)
    self.assertFalse(self.callback_func.called)
    self.assertEquals(op_timestamp, new_checkpoint)

  def test_tail_with_nonmatching_operation(self):
    op_timestamp = bson.Timestamp(long(time.time()), 0)
    op_message = {
      "ts" : op_timestamp,
      "op" : "u", #this is an update message
      "ns" : "foodb.barcol"
    }
    self.cursor.sort.return_value = [op_message]
    self.trigger.register("foodb.barcol", "i", self.callback_func) #only registering insert ("i") messages

    new_checkpoint = self.trigger._tail_oplog(self.default_checkpoint)

    self._assert_calls(self.default_checkpoint)
    self.assertFalse(self.callback_func.called)
    self.assertEquals(op_timestamp, new_checkpoint)

  def test_tail_with_wildcarding(self):
    op_timestamp = bson.Timestamp(long(time.time()), 0)
    #some oplog message I copied from Mongo
    op_message = {
      "ts" : op_timestamp,
      "h" : -2429474310205918006,
      "op" : "u",
      "ns" : "foodb.barcol",
      "o2" : {
        "_id" : "51d2daa81fa97fc9611102cf"
      },
      "o" : {
        "$set" : {
          "bar" : "baz"
        }
      }
    }
    self.cursor.sort.return_value = [op_message]
    self.trigger.register("foodb.*", "u", self.callback_func)

    new_checkpoint = self.trigger._tail_oplog(self.default_checkpoint)

    self._assert_calls(self.default_checkpoint)
    self.callback_func.assert_called_with(**op_message)
    self.assertEquals(op_timestamp, new_checkpoint)

class TriggersSetCheckpointTest(TestCase):

  def setUp(self):
    self.oplog = MagicMock()
    self.checkpoint_col = MagicMock()
    self.trigger = Triggers("my-source-id", self.oplog, self.checkpoint_col)

  def test_checkpoint_exists(self):
    self.checkpoint_col.find_one.return_value = {"checkpoint": "some value"}
    checkpoint = self.trigger._set_and_get_checkpoint()

    self.oplog.ensure_index.assert_called_with('ts')
    self.checkpoint_col.find_one.assert_called_with({"_id": "my-source-id"})
    self.assertEquals("some value", checkpoint)

  def test_checkpoint_does_not_exist(self):
    self.checkpoint_col.find_one.return_value = None
    checkpoint = self.trigger._set_and_get_checkpoint()

    self.oplog.ensure_index.assert_called_with('ts')
    self.checkpoint_col.find_one.assert_called_with({"_id": "my-source-id"})
    self.assertNotEquals("some value", checkpoint)
