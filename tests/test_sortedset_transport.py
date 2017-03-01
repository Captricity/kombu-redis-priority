from __future__ import unicode_literals

import unittest
import mock
import freezegun
import json
import time
import six

from .utils.fakeredis_ext import FakeStrictRedisWithConnection
from kombu import Connection
from kombu_redis_priority.transport.redis_priority_async import redis, Transport


class TestSortedSetTransport(unittest.TestCase):
    def setUp(self):
        self.faker = FakeStrictRedisWithConnection()
        with mock.patch.object(redis, 'StrictRedis', FakeStrictRedisWithConnection):
            self.connection = self.create_connection()
            self.channel = self.connection.default_channel

    def tearDown(self):
        self.faker.flushall()

    def create_connection(self):
        return Connection(transport=Transport)

    def _prefixed_message(self, time_, msg_obj):
        return six.b('{:011d}:'.format(int(time_)) + json.dumps(msg_obj))

    def test_default_message_add(self):
        raw_db = self.faker._db

        # assert no queues exist
        self.assertEqual(len(raw_db), 0)

        # put a blank message, locking the time
        with freezegun.freeze_time('1985-10-12'):
            faketime = time.time()
            self.channel._put('foo', {})

        # verify queue is created
        self.assertEqual(len(raw_db), 1)

        # ... and verify queue has a message
        raw_queue = raw_db['foo']
        self.assertEqual(len(raw_queue), 1)

        # verify message:
        # - a time prefix is appended to the message
        # - has default priority of +inf
        enqueued_msg, priority = next(six.iteritems(raw_queue))
        self.assertEqual(enqueued_msg, self._prefixed_message(faketime, {}))
        self.assertEqual(priority, float('+inf'))

    def test_prioritized_message_add(self):
        raw_db = self.faker._db
        msg = {'properties': {'zpriority': 5}}

        # assert no queues exist
        self.assertEqual(len(raw_db), 0)

        # put a blank message, locking the time
        with freezegun.freeze_time('1985-10-12'):
            faketime = time.time()
            self.channel._put('foo', msg)

        # verify queue is created
        self.assertEqual(len(raw_db), 1)

        # ... and verify queue has a message
        raw_queue = raw_db['foo']
        self.assertEqual(len(raw_queue), 1)

        # verify message:
        # - a time prefix is appended to the message
        # - has default priority (0)
        enqueued_msg, priority = next(six.iteritems(raw_queue))
        self.assertEqual(enqueued_msg, self._prefixed_message(faketime, msg))
        self.assertEqual(priority, 5.0)

    def test_zrem_read(self):
        # Add an item to create a queue
        msg = {
            'properties': {'delivery_tag': 'abcd'}
        }
        self.faker.zadd('foo', 1, self._prefixed_message(time.time(), msg))

        # Make the channel pull off the foo queue
        self.channel._active_queues.append('foo')
        self.channel._update_queue_cycle()

        # And then try the zrem pipeline
        self.channel._zrem_start()
        with mock.patch.object(self.channel.connection, '_deliver') as mock_deliver:
            self.channel._zrem_read()
            mock_deliver.assert_called_once_with(msg, 'foo')

    def test_purge(self):
        raw_db = self.faker._db

        # assert no queues exist
        self.assertEqual(len(raw_db), 0)

        # put a blank message
        self.channel._put('foo', {})
        # verify queue is created
        self.assertEqual(len(raw_db), 1)

        num_msg = self.channel._purge('foo')
        # verify that we removed one message and the key in Redis does not exist
        self.assertEqual(num_msg, 1)

    def test_size(self):
        size = self.channel._size('foo')
        # verify that there are no messages
        self.assertEqual(size, 0)

        # put two blank messages
        self.channel._put('foo', {'bar': 1})
        self.channel._put('foo', {'bar': 2})

        size = self.channel._size('foo')
        # verify that there are two messages
        self.assertEqual(size, 2)

    def test_has_queue(self):
        # put two blank messages
        self.channel._put('foo', {})

        self.assertTrue(self.channel._has_queue('foo'))
        self.assertFalse(self.channel._has_queue('bar'))
