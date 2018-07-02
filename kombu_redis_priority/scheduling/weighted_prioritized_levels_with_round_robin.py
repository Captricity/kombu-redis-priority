import random
from .base import QueueScheduler
from .prioritized_levels import PrioritizedLevelsQueueScheduler
from .round_robin import RoundRobinQueueScheduler

HIGHEST_LEVEL = float('inf')


class WeightedPrioritizedLevelsWithRRQueueScheduler(QueueScheduler):
    """
    Instance vars:
        weight_for_prioritized_levels : How often should we pull using
                                        prioritized levels. Should be a value
                                        between 0 and 1.
        prioritized_levels : PrioritizedLevelsQueueScheduler instance. Used
                             when coin flip chooses PrioritizedLevels.
        rr_queues : List of queues to round robin.
        round_robin : RoundRobinQueueScheduler instance. Used when coin flip
                      chooses RR.
        last_choice : What was the last chosen scheduler? Used to decide which
                      scheduler to rotate.
    """
    def __init__(
            self,
            weight_for_prioritized_levels,
            prioritized_levels_config,
            rr_queues):
        self.weight_for_prioritized_levels = weight_for_prioritized_levels
        self.prioritized_levels = \
            PrioritizedLevelsQueueScheduler(prioritized_levels_config)
        self.rr_queues = rr_queues
        self.round_robin = RoundRobinQueueScheduler()
        self.last_choice = None

    def next(self):
        if random.random() < self.weight_for_prioritized_levels:
            self.last_choice = self.prioritized_levels
            return self.prioritized_levels.next()
        else:
            self.last_choice = self.round_robin
            return self.round_robin.next()

    def rotate(self, *args):
        return self.last_choice.rotate(*args)

    def update(self, queue_list):
        self.prioritized_levels.update(queue_list)
        self.round_robin.update([q for q in queue_list if q in self.rr_queues])
