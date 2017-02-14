Usage
============

After configuring kombu-redis-priority, you can use it with Celery when applying tasks::

    your_task.apply_async(zpriority=1000)

The parameter zpriority is used instead of Celery's priority parameter to avoid confusion
with other Celery priority implementations. Lower values will have higher priority.

The default zpriority assigned to tasks is the lowest priority - +inf
