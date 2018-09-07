"""
This module contains various queue scheduling algorithms that can be used to
modify the behavior of redis priority transport in the face of multiple
queues.

This is an extension of kombu.utils.scheduling, to support schedulers that
require interactions with redis.
"""
import logging
logger = logging.getLogger('MOSTLY-PLS-TESTING')
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler('/var/log/pls_test.log'))
