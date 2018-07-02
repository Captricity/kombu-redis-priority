import unittest
import random
from kombu_redis_priority.scheduling.weighted_prioritized_levels_with_round_robin import \
    WeightedPrioritizedLevelsWithRRQueueScheduler

class TestPrioritizedLevelsWithRRQueueScheduler(unittest.TestCase):
    BASE_CONFIG = {
        0: ['TimeMachine', 'FluxCapacitor'],
        1: ['1985', '1955', '2015']
    }
    BASE_RR_QUEUES = ['1955', '2015']

    def test_weighted_choice_for_prioritized_levels(self):
        scheduler = \
            WeightedPrioritizedLevelsWithRRQueueScheduler(0.7, self.BASE_CONFIG, self.BASE_RR_QUEUES)
        random.seed(1985)  # next random.random is <0.7
        scheduler.next()
        self.assertEqual(scheduler.last_choice, scheduler.prioritized_levels)
        random.seed(1955)  # next random.random is <0.7
        scheduler.next()
        self.assertEqual(scheduler.last_choice, scheduler.round_robin)

    def test_prioritized_levels_is_properly_configured(self):
        scheduler = \
            WeightedPrioritizedLevelsWithRRQueueScheduler(0.7, self.BASE_CONFIG, self.BASE_RR_QUEUES)
        self.assertEqual(scheduler.prioritized_levels.queue_config, self.BASE_CONFIG)

    def test_round_robin_is_properly_configured(self):
        scheduler = \
            WeightedPrioritizedLevelsWithRRQueueScheduler(0.7, self.BASE_CONFIG, self.BASE_RR_QUEUES)
        scheduler.update(['1955'])
        q = scheduler.round_robin.next()
        self.assertEqual(q, '1955')
        scheduler.round_robin.rotate(q, False)
        q = scheduler.next()
        self.assertEqual(q, '1955')

    def test_scheduler_update_updates_prioritized_levels(self):
        scheduler = \
            WeightedPrioritizedLevelsWithRRQueueScheduler(0.7, self.BASE_CONFIG, self.BASE_RR_QUEUES)
        scheduler.update(['TimeMachine'])
        self.assertEqual(scheduler.prioritized_levels.queue_config, {0: ['TimeMachine']})
        self.assertEqual(scheduler.prioritized_levels.current_level, 0)
        self.assertEqual(scheduler.prioritized_levels.queue_cycle.items, ['TimeMachine'])
