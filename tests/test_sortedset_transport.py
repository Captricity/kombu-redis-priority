from __future__ import unicode_literals

import unittest
import mock
import freezegun
import json
import time
import six

from .utils.fakeredis_ext import FakeStrictRedisWithConnection
from fakeredis import FakeServer
from kombu import Connection
from kombu.five import Empty
from kombu_redis_priority.transport.redis_priority_async import Transport


class TestSortedSetTransport(unittest.TestCase):

    def setUp(self):
        self.fake_redis_server = FakeServer()
        self.fake_redis = FakeStrictRedisWithConnection(server=self.fake_redis_server)
        self.connection, self.channel = self.create_connection_and_channel(self.fake_redis, transport=Transport)

    def create_connection_and_channel(self, fake_redis, *args, **kwargs):
        path = "kombu_redis_priority.transport.redis_priority_async.redis.StrictRedis"
        with mock.patch(path, return_value=fake_redis):
            connection = Connection(*args, **kwargs)
            channel = connection.default_channel
        return connection, channel

    def _prefixed_message(self, time_, msg_obj):
        return six.b('{:011d}:'.format(int(time_)) + json.dumps(msg_obj))

    def test_default_message_add(self):
        # import ipdb
        # ipdb.set_trace()
        raw_db = self.fake_redis_server.dbs[0]
        # assert no queues exist
        self.assertEqual(len(raw_db), 0)

        # put a blank message, locking the time
        with freezegun.freeze_time('1985-10-12'):
            faketime = time.time()
            self.channel._put('foo', {})

        # verify queue is created
        self.assertEqual(len(raw_db), 1)

        # ... and verify queue has a message
        raw_queue = dict(raw_db[b'foo'].value.items())

        self.assertEqual(len(raw_queue), 1)

        # verify message:
        # - a time prefix is appended to the message
        # - has default priority of +inf
        enqueued_msg, priority = list(raw_queue.items())[0]
        self.assertEqual(enqueued_msg, self._prefixed_message(faketime, {}))
        self.assertEqual(priority, float('+inf'))

    def test_prioritized_message_add(self):
        raw_db = self.fake_redis_server.dbs[0]
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
        raw_queue = dict(raw_db[b'foo'].value.items())

        self.assertEqual(len(raw_queue), 1)

        # verify message:
        # - a time prefix is appended to the message
        # - has default priority (0)
        enqueued_msg, priority = list(raw_queue.items())[0]
        self.assertEqual(enqueued_msg, self._prefixed_message(faketime, msg))
        self.assertEqual(priority, 5.0)

    def test_zrem_read(self):
        # Add an item to create a queue
        msg = {
            'properties': {'delivery_tag': 'abcd'}
        }
        self.fake_redis.zadd('foo', {self._prefixed_message(time.time(), msg): 1})

        # Make the channel pull off the foo queue
        self.channel._active_queues.append('foo')
        self.channel._update_queue_schedule()

        # And then try the zrem pipeline
        self.channel._zrem_start()
        with mock.patch.object(self.channel.connection, '_deliver') as mock_deliver:
            self.channel._zrem_read()
            mock_deliver.assert_called_once_with(msg, 'foo')

    def test_purge(self):
        raw_db = self.fake_redis_server.dbs[0]

        # assert no queues exist
        self.assertEqual(len(raw_db), 0)

        # put a blank message
        self.channel._put('foo', {})
        # verify queue is created
        self.assertEqual(len(raw_db), 1)

        num_msg = self.channel._purge('foo')
        # verify that we removed one message and the key in Redis does not exist
        self.assertEqual(num_msg, 1)
        self.assertEqual(len(raw_db), 0)

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

    def test_round_robin_multiple_queues(self):
        # Create 2 queues with 2 messages
        msg = {
            'properties': {'delivery_tag': 'abcd'}
        }
        for i in range(2):
            self.fake_redis.zadd('foo', {self._prefixed_message(time.time() + i, msg): i})
            self.fake_redis.zadd('bar', {self._prefixed_message(time.time() + i, msg): i})

        self.channel._queue_scheduler.update(['foo', 'bar'])

        # And then check zrem pipeline rotates
        def check_zrem_pipeline(queue):
            self.channel._zrem_start()
            with mock.patch.object(self.channel.connection, '_deliver') as mock_deliver:
                self.channel._zrem_read()
                mock_deliver.assert_called_once_with(msg, queue)

        # Check two rotations
        check_zrem_pipeline('foo')
        check_zrem_pipeline('bar')
        check_zrem_pipeline('foo')
        check_zrem_pipeline('bar')

    def test_prioritized_levels_queue_scheduling_usage(self):
        # Setup channel with prioritized levels scheduler and queue preference
        queue_preference = {
            0: ['TimeMachine', 'FluxCapacitor'],
            1: ['1985', '1955', '2015'],
            2: ['Marty']
        }

        connection, channel = self.create_connection_and_channel(
            self.fake_redis,
            transport=Transport,
            transport_options={
                'queue_order_strategy': 'prioritized_levels',
                'prioritized_levels_queue_config': queue_preference
            }
        )

        # Setup so that only one queue in level 2 has a message
        msg = {
            'properties': {'delivery_tag': 'abcd'}
        }
        self.fake_redis.zadd('1955', {self._prefixed_message(time.time(), msg): 1})

        # Then check to make sure that scheduler will fully rotate levels 0 and 1, but not 2
        def check_zrem_pipeline(queue, empty):
            channel._zrem_start()
            with mock.patch.object(channel.connection, '_deliver') as mock_deliver:
                if empty:
                    with self.assertRaises(Empty):
                        channel._zrem_read()
                else:
                    channel._zrem_read()
                    mock_deliver.assert_called_once_with(msg, queue)

        check_zrem_pipeline('TimeMachine', True)
        check_zrem_pipeline('FluxCapacitor', True)
        check_zrem_pipeline('1985', True)
        check_zrem_pipeline('1955', False)
        check_zrem_pipeline('2015', True)
        check_zrem_pipeline('TimeMachine', True)

    def test_configuration_of_weighted_prioritized_levels_or_round_robin(self):
        # Setup channel with weighted prioritized levels scheduler and queue preference
        queue_preference = {
            0: ['TimeMachine', 'FluxCapacitor'],
            1: ['1985', '1955', '2015'],
            2: ['Marty']
        }
        weight = 0.7
        rr_queues = ['1985', '1955']

        connection, channel = self.create_connection_and_channel(
            self.fake_redis,
            transport=Transport,
            transport_options={
                'queue_order_strategy': 'weighted_prioritized_levels_with_round_robin',
                'prioritized_levels_queue_config': queue_preference,
                'weight_for_prioritized_levels': weight,
                'queues_for_round_robin': rr_queues
            }
        )

        scheduler = channel._queue_scheduler
        self.assertEqual(scheduler.prioritized_levels.queue_config, queue_preference)
        self.assertEqual(scheduler.weight_for_prioritized_levels, 0.7)
        self.assertEqual(scheduler.rr_queues, rr_queues)
