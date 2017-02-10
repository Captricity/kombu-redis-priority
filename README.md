# kombu-redis-priority

[![Build Status](https://travis-ci.org/Captricity/kombu-redis-priority.svg?branch=master)](https://travis-ci.org/Captricity/kombu-redis-priority) [![Coverage Status](https://coveralls.io/repos/Captricity/kombu-redis-priority/badge.png?branch=master)](https://coveralls.io/r/Captricity/kombu-redis-priority?branch=master)

Kombu Transport using Redis SortedSets

## Running tests

    python setup.py test

## Using with celery tasks - zpriority

    task.apply_async(zpriority=10)

zpriority is used to avoid confusion with other celery priority implementations

Note - tasks created using this backend will have the lowest priority, +inf
