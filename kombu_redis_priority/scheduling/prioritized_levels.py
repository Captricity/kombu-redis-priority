"""
Prioritized Levels scheduler for the queues.

Given a configuration of queues encoded as:
{
    LEVEL: [QUEUE]
}
where LEVEL is a numeric value indicating priority (smaller
first) and [QUEUE] indicates a list of queue names, the
scheduler will walk from smallest level to highest level,
only advancing levels if the smaller levels are empty.
Within levels, the queues are rotated in round robin
fashion.

To honor the prefernece for the lower levels, we will move
back to the lowest level when we do a full rotation of the
current level.

This is done to support the asynchronous nature in which
kombu pulls tasks.
"""
from collections import defaultdict
from kombu.utils.scheduling import cycle_by_name
from .base import QueueScheduler

HIGHEST_LEVEL = float('inf')


class PrioritizedLevelsQueueScheduler(QueueScheduler):
    """
    Instance vars:
        queue_cycle : kombu round_robin scheduler. Used to
                      implement round robin fashion within
                      levels.
        queue_config : Current preference list. Should only
                       contain queues that the worker should
                       pull from.
        current_level : Current level that worker is
                        scheduling queues from.
        start_of_rotation : The first queue in the rotation
                            for the current level. Used to
                            detect cycles.
        level_lookup_table : Lookup table mapping queues to
                             levels.
    """
    def __init__(self, config):
        self.queue_cycle = cycle_by_name('round_robin')()
        self.queue_config = config
        self._set_initial_state()

        self.level_lookup_table = {}
        for level, queues in config.items():
            for q in queues:
                self.level_lookup_table[q] = level

    def next(self):
        queues = self.queue_cycle.consume(1)
        if queues:
            return queues[0]
        return None

    def rotate(self, last_queue, was_empty):
        # This is first rotation for the level and queue was
        # empty, so start tracking that rotation was empty
        if last_queue == self.start_of_rotation and was_empty:
            self.rotation_empty = True
        # If at any time in rotation the queue was not
        # empty, then rotation is not empty
        elif not was_empty:
            self.rotation_empty = False

        # Rotate within the level and check if we fully
        # rotated the level.
        self.queue_cycle.rotate(last_queue)
        next_queue = self.queue_cycle.consume(1)[0]
        is_full_rotation = next_queue == self.start_of_rotation

        # On a full cycle and if full rotation was empty,
        # jump to next level
        if is_full_rotation and self.rotation_empty:
            next_index = (self.levels.index(self.current_level) + 1) % len(self.levels)
            self._set_level(self.levels[next_index])
            # In this situation, all queues are empty if
            # we were at the highest level
            return next_index == 0
        elif is_full_rotation:
            # otherwise, go back to lowest level
            self._set_level(min(self.levels))
        return False

    def update(self, queue_list):
        # Starting from base config, only include queues in
        # the provided list. For any queues not in the list,
        # set at the highest level (= lowest priority)
        config = defaultdict(list)
        for q in queue_list:
            if q in self.level_lookup_table:
                level = self.level_lookup_table[q]
            else:
                level = HIGHEST_LEVEL
            config[level].append(q)
        self.queue_config = config
        self._set_initial_state()

    def _set_initial_state(self):
        self.levels = sorted(self.queue_config.keys())
        if self.levels:
            self._set_level(min(self.levels))
        else:
            self._set_level(HIGHEST_LEVEL)

    def _set_level(self, level):
        self.current_level = level
        self.queue_cycle.update(self.queue_config[self.current_level])
        queues = self.queue_cycle.consume(1)
        if queues:
            self.start_of_rotation = queues[0]
        else:
            self.start_of_rotation = None
        self.rotation_empty = False
