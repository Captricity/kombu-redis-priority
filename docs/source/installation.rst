Installation
============

To get started using the redis_priority transport:

First, install the package::

    pip install kombu-redis-priority

Then, import the package at the start of your application, before you start
configuring kombu. For example in a Celery application, you would add the
following line before you configure your celery application in a celery.py file::

    import kombu_redis_priority.transport.redis_priority_async
    app = Celery('redis_priority_example')

You can now use the redis_priority transport by referring to the
`redispriorityasync` transport wherever you configure kombu.

Example::

    BROKER_URL = 'redispriorityasync://{}:{}/{}'.format(REDIS_HOST, REDIS_PORT, REDIS_QUEUE)

