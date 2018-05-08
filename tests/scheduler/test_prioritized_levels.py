import unittest

from kombu_redis_priority.scheduling.prioritized_levels import \
        HIGHEST_LEVEL, PrioritizedLevelsQueueScheduler


class TestPrioritizedLevelsQueueScheduler(unittest.TestCase):
    BASE_CONFIG = {
        0: ['TimeMachine', 'FluxCapacitor'],
        1: ['1985', '1955', '2015']
    }

    def test_prioritized_levels_scheduler_gets_queue_at_top_of_lowest_level(self):
        scheduler = PrioritizedLevelsQueueScheduler(self.BASE_CONFIG)
        self.assertEqual(scheduler.next(), 'TimeMachine')

    def test_prioritized_levels_scheduler_next_with_empty(self):
        scheduler = PrioritizedLevelsQueueScheduler(self.BASE_CONFIG)
        scheduler.update([])
        self.assertEqual(scheduler.next(), None)

    def test_prioritized_levels_scheduler_update_filters_out_queues_not_in_list(self):
        scheduler = PrioritizedLevelsQueueScheduler(self.BASE_CONFIG)
        scheduler.update(['TimeMachine'])
        self.assertEqual(scheduler.queue_config, {0: ['TimeMachine']})
        self.assertEqual(scheduler.current_level, 0)
        self.assertEqual(scheduler.queue_cycle.items, ['TimeMachine'])

    def test_prioritized_levels_scheduler_rotate_full_rotation_empty(self):
        scheduler = PrioritizedLevelsQueueScheduler(self.BASE_CONFIG)
        queues = ['TimeMachine', 'FluxCapacitor', '1985', '1955']
        for q in queues:
            self.assertEqual(scheduler.next(), q)
            self.assertFalse(scheduler.rotate(q, True))
        self.assertEqual(scheduler.next(), '2015')
        self.assertTrue(scheduler.rotate('2015', True))

    def test_prioritized_levels_scheduler_jumps_on_empty_full_rotation(self):
        scheduler = PrioritizedLevelsQueueScheduler(self.BASE_CONFIG)
        # full empty rotation on level 0 causes scheduler to jump to next level
        self.assertEqual(scheduler.current_level, 0)
        for q in self.BASE_CONFIG[0]:
            self.assertEqual(scheduler.next(), q)
            self.assertFalse(scheduler.rotate(q, True))
        self.assertEqual(scheduler.current_level, 1)
        self.assertEqual(scheduler.next(), '1985')

    def test_prioritized_levels_scheduler_fully_rotates_level(self):
        scheduler = PrioritizedLevelsQueueScheduler(self.BASE_CONFIG)
        scheduler._set_level(1)
        self.assertEqual(scheduler.next(), '1985')
        self.assertFalse(scheduler.rotate('1985', False))
        self.assertEqual(scheduler.next(), '1955')

    def test_prioritized_levels_scheduler_moves_to_lowest_level_when_consuming_higher_level_nonempty(self):
        config = self.BASE_CONFIG.copy()
        config[2] = ['Marty', 'Doc']
        scheduler = PrioritizedLevelsQueueScheduler(config)
        scheduler._set_level(1)
        for q in config[1]:
            self.assertEqual(scheduler.next(), q)
            self.assertFalse(scheduler.rotate(q, False))
        self.assertEqual(scheduler.current_level, 0)
        self.assertEqual(scheduler.next(), 'TimeMachine')

    def test_prioritized_levels_scheduler_update_non_existant_queue(self):
        scheduler = PrioritizedLevelsQueueScheduler(self.BASE_CONFIG)
        scheduler.update(['Marty'])
        self.assertEqual(scheduler.queue_config, {HIGHEST_LEVEL: ['Marty']})
