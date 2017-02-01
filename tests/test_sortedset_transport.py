from __future__ import unicode_literals

import unittest
import mock
import fakeredis
import freezegun
import json
import time
import six

from kombu import Connection
from kombu_redis_priority.transport.redis_priority_async import redis, Transport


class TestSortedSetTransport(unittest.TestCase):
    def setUp(self):
        self.faker = fakeredis.FakeStrictRedis()
        with mock.patch.object(redis, 'StrictRedis', fakeredis.FakeStrictRedis):
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
        # - has default priority (0)
        enqueued_msg, priority = next(six.iteritems(raw_queue))
        self.assertEqual(enqueued_msg, self._prefixed_message(faketime, {}))
        self.assertEqual(priority, 0.0)

    def test_prioritized_message_add(self):
        raw_db = self.faker._db
        msg = {'properties': {'priority': 5}}

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
