import unittest
from ddt import ddt, data

from kombu_redis_priority.scheduling.round_robin import RoundRobinQueueScheduler


@ddt
class TestRoundRobinQueueScheduler(unittest.TestCase):
    def test_round_robin_scheduler_gets_queue_at_top_of_list(self):
        scheduler = RoundRobinQueueScheduler()
        scheduler.update(['TimeMachine', 'FluxCapacitor'])
        self.assertEqual(scheduler.next(), 'TimeMachine')

    def test_round_robin_scheduler_next_with_empty(self):
        scheduler = RoundRobinQueueScheduler()
        scheduler.update([])
        self.assertEqual(scheduler.next(), None)

    def test_round_robin_scheduler_update_sets_internal_list(self):
        scheduler = RoundRobinQueueScheduler()
        scheduler.update(['TimeMachine', 'FluxCapacitor'])
        self.assertEqual(scheduler.cycle.items, ['TimeMachine', 'FluxCapacitor'])

    @data(True, False)
    def test_round_robin_scheduler_rotate_rotates_queue_regardless_of_emptiness(self, was_empty):
        scheduler = RoundRobinQueueScheduler()
        scheduler.update(['TimeMachine', 'FluxCapacitor'])
        scheduler.rotate('TimeMachine', was_empty)
        self.assertEqual(scheduler.cycle.items, ['FluxCapacitor', 'TimeMachine'])

    def test_round_robin_scheduler_rotate_full_rotation_empty(self):
        scheduler = RoundRobinQueueScheduler()
        scheduler.update(['TimeMachine', 'FluxCapacitor'])
        # Have not made a full rotation, not fully empty yet
        self.assertFalse(scheduler.rotate('TimeMachine', True))
        # Made a full round trip and both queues were empty
        self.assertTrue(scheduler.rotate('FluxCapacitor', True))

    def test_round_robin_scheduler_rotate_full_rotation_state_tracking(self):
        scheduler = RoundRobinQueueScheduler()
        scheduler.update(['TimeMachine', 'FluxCapacitor', 'Delorean'])
        # Have not made a full rotation, not fully empty yet
        self.assertFalse(scheduler.rotate('TimeMachine', True))
        self.assertFalse(scheduler.rotate('FluxCapacitor', True))
        # Made a full rotation, but the last queue was not empty
        self.assertFalse(scheduler.rotate('Delorean', False))
