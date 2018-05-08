class QueueScheduler(object):
    def next(self):
        """Return the next queue to consume from."""
        raise NotImplementedError

    def rotate(self, last_queue, was_empty):
        """
        Rotate queue list based on what queue was popped
        last and whether or not it was empty.

        Args:
            last_queue : Queue that was returned by scheduler and consumed from.
            was_empty (bool) : Whether or not the last_queue was empty when consumed.

        Returns:
            True when a full rotation was made and all the queues were empty.
        """
        raise NotImplementedError

    def update(self, queue_list):
        """
        Update internal queue list with the list of queues
        to consume from.
        """
        raise NotImplementedError
