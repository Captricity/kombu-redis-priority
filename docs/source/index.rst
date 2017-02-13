.. kombu_redis_priority documentation master file, created by
   sphinx-quickstart on Fri Feb  3 09:34:22 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Kombu Redis Priority plugin
===========================

kombu_redis_priority is a plugin into kombu that provides a Redis backed
transport backend that supports prioritized ordering of messages through
SortedSet.

kombu_redis_priority works by extending the existing redis transport backend
and replacing references to the list data type with sortedset. It then utilizes
the priority interface provided in AMQP to order the messages within the
sortedset, thereby allowing implementations of prioritized messaging.

In addition, it takes advantage of lexicographical ordering of messages to
achieve FIFO queueing when the priorities are equivalent. This is achieved by
prefixing message jsons with epoch timestamps at the time of enqueueing
messages into the sortedset.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   testing


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
