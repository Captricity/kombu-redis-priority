"""
RoundRobin scheduler for the queues, evenly cycling through the list of
queues to consume from.
"""
from kombu.utils.scheduling import cycle_by_name
from .base import QueueScheduler


class RoundRobinQueueScheduler(QueueScheduler):
    def __init__(self):
        self.cycle = cycle_by_name('round_robin')()

    def next(self):
        queues = self.cycle.consume(1)
        if queues:
            return queues[0]
        return None

    def rotate(self, last_queue, was_empty):
        # This is first rotation and queue was empty, so
        # start tracking that rotation was empty
        if last_queue == self.start_queue and was_empty:
            self.rotation_empty = True
        # If at any time in rotation the queue was not
        # empty, then rotation is not empty
        elif not was_empty:
            self.rotation_empty = False

        self.cycle.rotate(last_queue)
        next_queue = self.next()
        is_full_rotation = next_queue == self.start_queue
        return is_full_rotation and self.rotation_empty

    def update(self, queue_list):
        self.cycle.update(queue_list)
        self.start_queue = self.next()
