Queue Scheduling
================

By default the `redis_priority` transport will consume from multiple queues in
a round robin fashion, like all the other transports for `kombu`. Currently,
this transport does not support the other strategies specified in `kombu`
(`priority` and `sorted`). Instead, this transport provides the
`prioritized_levels` strategy described below.

Prioritized Scheduling
----------------------

Given a configuration of queues encoded as::

    {
        LEVEL: [QUEUE]
    }

where `LEVEL` is a numeric value indicating priority (smaller
first) and `[QUEUE]` indicates a list of queue names, the
scheduler will walk from smallest level to highest level,
only advancing levels if the smaller levels are empty.
Within levels, the queues are rotated in round robin
fashion.

To honor the prefernece for the lower levels, we will move
back to the lowest level when we do a full rotation of the
current level.

You can configure the `redis_priority` transport to use this method by using
the `queue_order_strategy` and `prioritized_levels_queue_config` transport
options, configured with `BROKER_TRANSPORT_OPTIONS`.

Example::

    BROKER_TRANSPORT_OPTIONS = {
        'queue_order_strategy': 'prioritized_levels',
        'prioritized_levels_queue_config': {
            0: ['TimeMachine', 'FluxCapacitor'],
            1: ['1985', '1955', '2015']
        }
    }

Note that any queue that the worker is specified to consume which is not in the
`prioritized_levels_queue_config` is automatically specified at the highest
level (max int).
