"""Redis transport backed by sortedset data structure."""
from __future__ import absolute_import, unicode_literals

import numbers
from bisect import bisect
from contextlib import contextmanager
from itertools import chain
from time import time, sleep

from kombu import transport
from vine import promise

from kombu.exceptions import InconsistencyError, VersionMismatch
from kombu.five import Empty, values, string_t
from kombu.log import get_logger
from kombu.utils.compat import register_after_fork
from kombu.utils.eventio import poll, READ, ERR
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import loads, dumps
from kombu.utils.objects import cached_property
from kombu.utils.scheduling import cycle_by_name
from kombu.utils.url import _parse_url

from kombu.transport.redis import NO_ROUTE_ERROR, get_redis_error_classes, PRIORITY_STEPS
from kombu.transport.redis import _after_fork_cleanup_channel, QoS

import kombu.transport.virtual as virtual

from ..scheduling.round_robin import RoundRobinQueueScheduler
from ..scheduling.prioritized_levels import PrioritizedLevelsQueueScheduler

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None     # noqa


# Register priority transport into available transports under kombu
transport.TRANSPORT_ALIASES['redispriorityasync'] = 'kombu_redis_priority.transport.redis_priority_async:Transport'


logger = get_logger('kombu.transport.redis_priority')
crit, warn = logger.critical, logger.warn

DEFAULT_PORT = 6379
DEFAULT_DB = 0


# Copied from kombu.transport.redis, with list operations replaced with
# sortedset operations. Below notes are copied verbatim from
# kombu.transport.redis as the same comments apply here.
# -----------------
# This implementation may seem overly complex, but I assure you there is
# a good reason for doing it this way.
#
# Consuming from several connections enables us to emulate channels,
# which means we can have different service guarantees for individual
# channels.
#
# So we need to consume messages from multiple connections simultaneously,
# and using epoll means we don't have to do so using multiple threads.
#
# Also it means we can easily use PUBLISH/SUBSCRIBE to do fanout
# exchanges (broadcast), as an alternative to pushing messages to fanout-bound
# queues manually. Note that we must support the fanout exchanges to support
# the celery events system.


class MultiChannelPoller(object):
    """Async I/O poller for Redis transport."""

    eventflags = READ | ERR

    #: Set by :meth:`get` while reading from the socket.
    _in_protected_read = False

    #: Set of one-shot callbacks to call after reading from socket.
    after_read = None

    def __init__(self):
        # active channels
        self._channels = set()
        # file descriptor -> channel map.
        self._fd_to_chan = {}
        # channel -> socket map
        self._chan_to_sock = {}
        # poll implementation (epoll/kqueue/select)
        self.poller = poll()
        # one-shot callbacks called after reading from socket.
        self.after_read = set()

    def close(self):
        for fd in values(self._chan_to_sock):
            try:
                self.poller.unregister(fd)
            except (KeyError, ValueError):
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()

    def add(self, channel):
        self._channels.add(channel)

    def discard(self, channel):
        self._channels.discard(channel)

    def _on_connection_disconnect(self, connection):
        try:
            self.poller.unregister(connection._sock)
        except (AttributeError, TypeError):
            pass

    def _register(self, channel, client, type):
        if (channel, client, type) in self._chan_to_sock:
            self._unregister(channel, client, type)
        if client.connection._sock is None:   # not connected yet.
            client.connection.connect()
        sock = client.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, type)
        self._chan_to_sock[(channel, client, type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, type):
        self.poller.unregister(self._chan_to_sock[(channel, client, type)])

    def _client_registered(self, channel, client, cmd):
        if getattr(client, 'connection', None) is None:
            client.connection = client.connection_pool.get_connection('_')
        return (client.connection._sock is not None and
                (channel, client, cmd) in self._chan_to_sock)

    def _register_ZREM(self, channel):
        """Enable ZREM mode for channel."""
        ident = channel, channel.client, 'ZREM'
        if not self._client_registered(channel, channel.client, 'ZREM'):
            channel._in_poll = False
            self._register(*ident)
        if not channel._in_poll:  # send ZREM commands
            channel._zrem_start()

    def _register_LISTEN(self, channel):
        """Enable LISTEN mode for channel."""
        if not self._client_registered(channel, channel.subclient, 'LISTEN'):
            channel._in_listen = False
            self._register(channel, channel.subclient, 'LISTEN')
        if not channel._in_listen:
            channel._subscribe()  # send SUBSCRIBE

    def on_poll_start(self):
        for channel in self._channels:
            if channel.active_queues:           # ZREM mode?
                if channel.qos.can_consume():
                    self._register_ZREM(channel)
            if channel.active_fanout_queues:    # LISTEN mode?
                self._register_LISTEN(channel)

    def on_poll_init(self, poller):
        self.poller = poller
        for channel in self._channels:
            return channel.qos.restore_visible(
                num=channel.unacked_restore_limit,
            )

    def maybe_restore_messages(self):
        for channel in self._channels:
            if channel.active_queues:
                # only need to do this once, as they are not local to channel.
                return channel.qos.restore_visible(
                    num=channel.unacked_restore_limit,
                )

    def on_readable(self, fileno):
        chan, type = self._fd_to_chan[fileno]
        if chan.qos.can_consume():
            chan.handlers[type]()

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, type = self._fd_to_chan[fileno]
            chan._poll_error(type)

    def get(self, callback, timeout=None):
        self._in_protected_read = True
        try:
            for channel in self._channels:
                if channel.active_queues:           # ZREM mode?
                    if channel.qos.can_consume():
                        self._register_ZREM(channel)
                if channel.active_fanout_queues:    # LISTEN mode?
                    self._register_LISTEN(channel)

            events = self.poller.poll(timeout)
            if events:
                for fileno, event in events:
                    ret = self.handle_event(fileno, event)
                    if ret:
                        return
            # - no new data, so try to restore messages.
            # - reset active redis commands.
            self.maybe_restore_messages()
            raise Empty()
        finally:
            self._in_protected_read = False
            while self.after_read:
                try:
                    fun = self.after_read.pop()
                except KeyError:
                    break
                else:
                    fun()

    @property
    def fds(self):
        return self._fd_to_chan


class Channel(virtual.Channel):
    """Redis Channel."""

    QoS = QoS

    _client = None
    _subclient = None
    _closing = False
    supports_fanout = True
    keyprefix_queue = '_kombu.binding.%s'
    keyprefix_fanout = '/{db}.'
    sep = '\x06\x16'
    _in_poll = False
    _in_listen = False
    _fanout_queues = {}
    ack_emulation = True
    unacked_key = 'unacked'
    unacked_index_key = 'unacked_index'
    unacked_mutex_key = 'unacked_mutex'
    unacked_mutex_expire = 300  # 5 minutes
    unacked_restore_limit = None
    visibility_timeout = 3600   # 1 hour
    priority_steps = PRIORITY_STEPS
    socket_timeout = None
    socket_connect_timeout = None
    socket_keepalive = None
    socket_keepalive_options = None
    max_connections = 10
    #: Transport option to disable fanout keyprefix.
    #: Can also be string, in which case it changes the default
    #: prefix ('/{db}.') into to something else.  The prefix must
    #: include a leading slash and a trailing dot.
    #:
    #: Enabled by default since Kombu 4.x.
    #: Disable for backwards compatibility with Kombu 3.x.
    fanout_prefix = True

    #: If enabled the fanout exchange will support patterns in routing
    #: and binding keys (like a topic exchange but using PUB/SUB).
    #:
    #: Enabled by default since Kombu 4.x.
    #: Disable for backwards compatibility with Kombu 3.x.
    fanout_patterns = True

    #: Order in which we consume from queues.
    #:
    #: Can be either string alias, or a cycle strategy class
    #:
    #: - ``round_robin``
    #:   (:class:`~kombu_redis_priority.scheduling.round_robin`).
    #:
    #:    Make sure each queue has an equal opportunity to be consumed from.
    #:
    #: - ``prioritized_levels``
    #:   (:class:`~kombu_redis_priority.scheduling.prioritized_levels`).
    #:
    #:    Requires setting prioritized_levels_queue_config.
    #:
    #: The default is to consume from queues in round robin.
    queue_order_strategy = 'round_robin'
    prioritized_levels_queue_config = {}
    empty = False

    _async_pool = None
    _pool = None

    from_transport_options = (
        virtual.Channel.from_transport_options +
        ('ack_emulation',
         'unacked_key',
         'unacked_index_key',
         'unacked_mutex_key',
         'unacked_mutex_expire',
         'visibility_timeout',
         'unacked_restore_limit',
         'fanout_prefix',
         'fanout_patterns',
         'socket_timeout',
         'socket_connect_timeout',
         'socket_keepalive',
         'socket_keepalive_options',
         'queue_order_strategy',
         'max_connections',
         'priority_steps',
         'prioritized_levels_queue_config')  # <-- do not add comma here!
    )

    default_zpriority = '+inf'

    connection_class = redis.Connection if redis else None

    def __init__(self, *args, **kwargs):
        super_ = super(Channel, self)
        super_.__init__(*args, **kwargs)

        if not self.ack_emulation:  # disable visibility timeout
            self.QoS = virtual.QoS

        if self.queue_order_strategy == 'round_robin':
            self._queue_scheduler = RoundRobinQueueScheduler()
        elif self.queue_order_strategy == 'prioritized_levels':
            self._queue_scheduler = \
                PrioritizedLevelsQueueScheduler(self.prioritized_levels_queue_config)
        else:
            raise NotImplementedError
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues = set()
        self.auto_delete_queues = set()
        self._fanout_to_queue = {}
        self.handlers = {'ZREM': self._zrem_read, 'LISTEN': self._receive}

        if self.fanout_prefix:
            if isinstance(self.fanout_prefix, string_t):
                self.keyprefix_fanout = self.fanout_prefix
        else:
            # previous versions did not set a fanout, so cannot enable
            # by default.
            self.keyprefix_fanout = ''

        # Evaluate connection.
        try:
            self.client.ping()
        except Exception:
            self._disconnect_pools()
            raise

        self.connection.cycle.add(self)  # add to channel poller.
        # copy errors, in case channel closed but threads still
        # are still waiting for data.
        self.connection_errors = self.connection.connection_errors

        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_channel)

    def _after_fork(self):
        self._disconnect_pools()

    def _disconnect_pools(self):
        pool = self._pool
        async_pool = self._async_pool

        self._async_pool = self._pool = None

        if pool is not None:
            pool.disconnect()

        if async_pool is not None:
            async_pool.disconnect()

    def _on_connection_disconnect(self, connection):
        if self._in_poll is connection:
            self._in_poll = None
        if self._in_listen is connection:
            self._in_listen = None
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    def _do_restore_message(self, payload, exchange, routing_key,
                            client=None, leftmost=False):
        with self.conn_or_acquire(client) as client:
            try:
                try:
                    payload['headers']['redelivered'] = True
                except KeyError:
                    pass
                for queue in self._lookup(exchange, routing_key):
                    # Add with priority 0 so it jumps ahead in the queue
                    client.zadd(queue, '-inf', self._add_time_prefix(dumps(payload)))
            except Exception:
                crit('Could not restore message: %r', payload, exc_info=True)

    def _restore(self, message, leftmost=False):
        if not self.ack_emulation:
            return super(Channel, self)._restore(message)
        tag = message.delivery_tag
        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                P, _ = pipe.hget(self.unacked_key, tag) \
                           .hdel(self.unacked_key, tag) \
                           .execute()
            if P:
                M, EX, RK = loads(bytes_to_str(P))  # json is unicode
                self._do_restore_message(M, EX, RK, client, leftmost)

    def _restore_at_beginning(self, message):
        return self._restore(message, leftmost=True)

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange, _ = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        ret = super(Channel, self).basic_consume(queue, *args, **kwargs)

        # TODO: update documentation to reflect sortedset implementation
        # (we are not using BRPOP)
        #
        # Update fair cycle between queues.
        #
        # We cycle between queues fairly to make sure that
        # each queue is equally likely to be consumed from,
        # so that a very busy queue will not block others.
        #
        # This works by using Redis's `BRPOP` command and
        # by rotating the most recently used queue to the
        # and of the list.  See Kombu github issue #166 for
        # more discussion of this method.
        self._update_queue_schedule()
        return ret

    def basic_cancel(self, consumer_tag):
        # If we are busy reading messages we may experience
        # a race condition where a message is consumed after
        # canceling, so we must delay this operation until reading
        # is complete (Issue celery/celery#1773).
        connection = self.connection
        if connection:
            if connection.cycle._in_protected_read:
                return connection.cycle.after_read.add(
                    promise(self._basic_cancel, (consumer_tag,)),
                )
            return self._basic_cancel(consumer_tag)

    def _basic_cancel(self, consumer_tag):
        try:
            queue = self._tag_to_queue[consumer_tag]
        except KeyError:
            return
        try:
            self.active_fanout_queues.remove(queue)
        except KeyError:
            pass
        else:
            self._unsubscribe_from(queue)
        try:
            exchange, _ = self._fanout_queues[queue]
            self._fanout_to_queue.pop(exchange)
        except KeyError:
            pass
        ret = super(Channel, self).basic_cancel(consumer_tag)
        self._update_queue_schedule()
        return ret

    def _get_publish_topic(self, exchange, routing_key):
        if routing_key and self.fanout_patterns:
            return ''.join([self.keyprefix_fanout, exchange, '/', routing_key])
        return ''.join([self.keyprefix_fanout, exchange])

    def _get_subscribe_topic(self, queue):
        exchange, routing_key = self._fanout_queues[queue]
        return self._get_publish_topic(exchange, routing_key)

    def _subscribe(self):
        keys = [self._get_subscribe_topic(queue)
                for queue in self.active_fanout_queues]
        if not keys:
            return
        c = self.subclient
        if c.connection._sock is None:
            c.connection.connect()
        self._in_listen = c.connection
        c.psubscribe(keys)

    def _unsubscribe_from(self, queue):
        topic = self._get_subscribe_topic(queue)
        c = self.subclient
        if c.connection and c.connection._sock:
            c.unsubscribe([topic])

    def _handle_message(self, client, r):
        if bytes_to_str(r[0]) == 'unsubscribe' and r[2] == 0:
            client.subscribed = False
            return

        if bytes_to_str(r[0]) == 'pmessage':
            type, pattern, channel, data = r[0], r[1], r[2], r[3]
        else:
            type, pattern, channel, data = r[0], None, r[1], r[2]
        return {
            'type': type,
            'pattern': pattern,
            'channel': channel,
            'data': data,
        }

    def _receive(self):
        c = self.subclient
        ret = []
        try:
            ret.append(self._receive_one(c))
        except Empty:
            pass
        if c.connection is not None:
            while c.connection.can_read(timeout=0):
                ret.append(self._receive_one(c))
        return any(ret)

    def _receive_one(self, c):
        response = None
        try:
            response = c.parse_response()
        except self.connection_errors:
            self._in_listen = None
            raise
        if response is not None:
            payload = self._handle_message(c, response)
            if bytes_to_str(payload['type']).endswith('message'):
                channel = bytes_to_str(payload['channel'])
                if payload['data']:
                    if channel[0] == '/':
                        _, _, channel = channel.partition('.')
                    try:
                        message = loads(bytes_to_str(payload['data']))
                    except (TypeError, ValueError):
                        warn('Cannot process event on channel %r: %s',
                             channel, repr(payload)[:4096], exc_info=1)
                        raise Empty()
                    exchange = channel.split('/', 1)[0]
                    self.connection._deliver(
                        message, self._fanout_to_queue[exchange])
                    return True

    def _zrem_start(self, timeout=1):
        queue = self._queue_scheduler.next()
        if not queue:
            return
        self.queue = queue

        self._in_poll = self.client.connection

        # In one atomic pipe, grab the top element in the queue by
        # score (ZRANGE) and remove it (ZREMRANGEBYRANK)
        pipe = self.client.pipeline()
        pipe.zrange(queue, 0, 0)
        pipe.zremrangebyrank(queue, 0, 0)

        # Hack to make call asynchronous
        connection = self.client.connection
        # Wrap pipeline commands in MULTI/EXEC so that they are executed
        # atomically by redis.
        cmds = chain([(('MULTI', ), {})], pipe.command_stack, [(('EXEC', ), {})])
        all_cmds = connection.pack_commands([args for args, _ in cmds])
        connection.send_packed_command(all_cmds)

    def _zrem_read(self, **options):
        try:
            try:
                # The last response contains the response of ZRANGE.
                output = None
                for i in range(0, 4):
                    output = self.client.parse_response(self.client.connection, '_', **options)
                item = output[0]
                if len(item) == 0:
                    item = None
                else:
                    item = self._remove_time_prefix(item[0])

            except self.connection_errors:
                # if there's a ConnectionError, disconnect so the next
                # iteration will reconnect automatically.
                self.client.connection.disconnect()
                raise

            # Rotate the queue
            dest = self.queue
            all_queues_empty = self._queue_scheduler.rotate(dest, not bool(item))

            if item:
                self.connection._deliver(loads(bytes_to_str(item)), dest)
                return True
            elif all_queues_empty:
                sleep(1)
            raise Empty()
        finally:
            self._in_poll = None

    def _poll_error(self, type, **options):
        if type == 'LISTEN':
            self.subclient.parse_response()
        else:
            self.client.parse_response(self.client.connection, type)

    def _get(self, queue):
        # This method is used with a synchronous Kombu channel, which is not currently supported.
        # Look at commit 4f956903cecc9d575193b7b0819ebe4a386328c9 to the view the old list backend code.
        raise NotImplementedError

    def _size(self, queue):
        with self.conn_or_acquire() as client:
            return client.zcard(queue)

    def _put(self, queue, message, **kwargs):
        """Deliver message."""
        pri = self._get_message_priority(message, reverse=False)

        with self.conn_or_acquire() as client:
            client.zadd(queue, pri, self._add_time_prefix(dumps(message)))

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Deliver fanout message."""
        with self.conn_or_acquire() as client:
            client.publish(
                self._get_publish_topic(exchange, routing_key),
                dumps(message),
            )

    def _new_queue(self, queue, auto_delete=False, **kwargs):
        if auto_delete:
            self.auto_delete_queues.add(queue)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == 'fanout':
            # Mark exchange as fanout.
            self._fanout_queues[queue] = (
                exchange, routing_key.replace('#', '*'),
            )
        with self.conn_or_acquire() as client:
            client.sadd(self.keyprefix_queue % (exchange,),
                        self.sep.join([routing_key or '',
                                       pattern or '',
                                       queue or '']))

    def _delete(self, queue, exchange, routing_key, pattern, *args, **kwargs):
        self.auto_delete_queues.discard(queue)
        with self.conn_or_acquire(client=kwargs.get('client')) as client:
            client.srem(self.keyprefix_queue % (exchange,),
                        self.sep.join([routing_key or '',
                                       pattern or '',
                                       queue or '']))
            client.delete(queue)

    def _has_queue(self, queue, **kwargs):
        with self.conn_or_acquire() as client:
            return client.exists(queue)

    def get_table(self, exchange):
        key = self.keyprefix_queue % exchange
        with self.conn_or_acquire() as client:
            values = client.smembers(key)
            if not values:
                raise InconsistencyError(NO_ROUTE_ERROR.format(exchange, key))
            return [tuple(bytes_to_str(val).split(self.sep)) for val in values]

    def _purge(self, queue):
        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                pipe = pipe.zcard(queue).delete(queue)
                size = pipe.execute()
                return size[0]

    def close(self):
        self._closing = True
        if not self.closed:
            # remove from channel poller.
            self.connection.cycle.discard(self)

            # delete fanout bindings
            client = self.__dict__.get('client')  # only if property cached
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
            self._disconnect_pools()
            self._close_clients()
        super(Channel, self).close()

    def _close_clients(self):
        # Close connections
        for attr in 'client', 'subclient':
            try:
                client = self.__dict__[attr]
                connection, client.connection = client.connection, None
                connection.disconnect()
            except (KeyError, AttributeError, self.ResponseError):
                pass

    def _prepare_virtual_host(self, vhost):
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == '/':
                vhost = DEFAULT_DB
            elif vhost.startswith('/'):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError(
                    'Database is int between 0 and limit - 1, not {0}'.format(
                        vhost,
                    ))
        return vhost

    def _filter_tcp_connparams(self, socket_keepalive=None,
                               socket_keepalive_options=None, **params):
        return params

    def _connparams(self, async=False):
        conninfo = self.connection.client
        connparams = {
            'host': conninfo.hostname or '127.0.0.1',
            'port': conninfo.port or self.connection.default_port,
            'virtual_host': conninfo.virtual_host,
            'password': conninfo.password,
            'max_connections': self.max_connections,
            'socket_timeout': self.socket_timeout,
            'socket_connect_timeout': self.socket_connect_timeout,
            'socket_keepalive': self.socket_keepalive,
            'socket_keepalive_options': self.socket_keepalive_options,
        }
        if conninfo.ssl:
            # Connection(ssl={}) must be a dict containing the keys:
            # 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
            try:
                connparams.update(conninfo.ssl)
                connparams['connection_class'] = redis.SSLConnection
            except TypeError:
                pass
        host = connparams['host']
        if '://' in host:
            scheme, _, _, _, _, path, query = _parse_url(host)
            if scheme == 'socket':
                connparams = self._filter_tcp_connparams(**connparams)
                connparams.update({
                    'connection_class': redis.UnixDomainSocketConnection,
                    'path': '/' + path}, **query)

                connparams.pop('socket_connect_timeout', None)
                connparams.pop('socket_keepalive', None)
                connparams.pop('socket_keepalive_options', None)

            connparams.pop('host', None)
            connparams.pop('port', None)
        connparams['db'] = self._prepare_virtual_host(
            connparams.pop('virtual_host', None))

        channel = self
        connection_cls = (
            connparams.get('connection_class') or
            self.connection_class
        )

        if async:
            class Connection(connection_cls):
                def disconnect(self):
                    super(Connection, self).disconnect()
                    channel._on_connection_disconnect(self)
            connection_cls = Connection

        connparams['connection_class'] = connection_cls

        return connparams

    def _create_client(self, async=False):
        if async:
            return self.Client(connection_pool=self.async_pool)
        return self.Client(connection_pool=self.pool)

    def _get_pool(self, async=False):
        params = self._connparams(async=async)
        self.keyprefix_fanout = self.keyprefix_fanout.format(db=params['db'])
        return redis.ConnectionPool(**params)

    def _get_client(self):
        if redis.VERSION < (2, 10, 0):
            raise VersionMismatch(
                'Redis transport requires redis-py versions 2.10.0 or later. '
                'You have {0.__version__}'.format(redis))
        return redis.StrictRedis

    @contextmanager
    def conn_or_acquire(self, client=None):
        if client:
            yield client
        else:
            yield self._create_client()

    @property
    def pool(self):
        if self._pool is None:
            self._pool = self._get_pool()
        return self._pool

    @property
    def async_pool(self):
        if self._async_pool is None:
            self._async_pool = self._get_pool(async=True)
        return self._async_pool

    @cached_property
    def client(self):
        """Client used to publish messages, ZREM etc."""
        return self._create_client(async=True)

    @cached_property
    def subclient(self):
        """Pub/Sub connection used to consume fanout queues."""
        client = self._create_client(async=True)
        return client.pubsub()

    def _update_queue_schedule(self):
        self._queue_scheduler.update(self.active_queues)

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    @property
    def active_queues(self):
        """Set of queues being consumed from (excluding fanout queues)."""
        return {queue for queue in self._active_queues
                if queue not in self.active_fanout_queues}

    def _get_message_priority(self, message, reverse=False):
        """Get priority from message key zpriority

        Uses zpriority instead of priority to differentiate
        from other celery priority implementations

        The value is not limited!

        Note:
            Lower value has more priority.
        """
        try:
            priority = int(message['properties']['zpriority'])
        except (TypeError, ValueError, KeyError):
            priority = self.default_zpriority

        return priority

    def _add_time_prefix(self, message):
        # Add an prefix representing the current time, so that messages with the same priority have FIFO behavior
        return '{:011d}:'.format(int(time())) + message

    def _remove_time_prefix(self, message):
        return message[12:]


class Transport(virtual.Transport):
    """Redis Transport."""

    Channel = Channel

    polling_interval = 1.0  # disable sleep between unsuccessful polls.
    default_port = DEFAULT_PORT
    driver_type = 'redis'
    driver_name = 'redis'

    implements = virtual.Transport.implements.extend(
        async=True,
        exchange_type=frozenset(['direct', 'topic', 'fanout'])
    )

    def __init__(self, *args, **kwargs):
        if redis is None:
            raise ImportError('Missing redis library (pip install redis)')
        super(Transport, self).__init__(*args, **kwargs)

        # Get redis-py exceptions.
        self.connection_errors, self.channel_errors = self._get_errors()
        # All channels share the same poller.
        self.cycle = MultiChannelPoller()

    def driver_version(self):
        return redis.__version__

    def register_with_event_loop(self, connection, loop):
        cycle = self.cycle
        cycle.on_poll_init(loop.poller)
        cycle_poll_start = cycle.on_poll_start
        add_reader = loop.add_reader
        on_readable = self.on_readable

        def _on_disconnect(connection):
            if connection._sock:
                loop.remove(connection._sock)
        cycle._on_connection_disconnect = _on_disconnect

        def on_poll_start():
            cycle_poll_start()
            [add_reader(fd, on_readable, fd) for fd in cycle.fds]
        loop.on_tick.add(on_poll_start)
        loop.call_repeatedly(10, cycle.maybe_restore_messages)

    def on_readable(self, fileno):
        """Handle AIO event for one of our file descriptors."""
        self.cycle.on_readable(fileno)

    def _get_errors(self):
        """Utility to import redis-py's exceptions at runtime."""
        return get_redis_error_classes()
