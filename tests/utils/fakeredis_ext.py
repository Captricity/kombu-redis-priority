""" Extensions for fakeredis, namely adding connection interface that is used by kombu """

from collections import deque
from itertools import count
from fakeredis import FakeStrictRedis, FakePipeline


class FakeStrictRedisWithConnection(FakeStrictRedis):
    """
    An extension of FakeStrictRedis to implement some of the low level interfaces of StrictRedis from redis-py. Kombu
    uses these internal features to simulate an async event based request response cycle so that it can be hooked into
    its chain.

    You can learn more about it in the kombu source for the redis transport.
    """
    def __init__(self, *args, **kwargs):
        super(FakeStrictRedisWithConnection, self).__init__(*args, **kwargs)
        self._connection = None
        self.connection = self._sconnection(self)
        self._response_queue = deque()

    def parse_response(self, connection, type, **options):
        # If there are any responses queued up, pop and return that
        if self._response_queue:
            return self._response_queue.pop()

        # TODO: this is actually wrong - we need to determine if it is a pipeline response based on what is on the
        #       datagram.
        if type == '_':
            return self._parse_pipeline_response_from_connection(connection)
        else:
            return self._parse_command_response_from_connection(connection, type)

    def _parse_pipeline_response_from_connection(self, connection):
        """
        A pipeline response consists of several responses:
        - OK : acknowledges a transaction
        - QUEUED : acknowledges a command has been queued. There will be one per command sent.
        - LIST : list of responses
        """
        # pop off the first command, which should be MULTI to signal start of transaction
        cmd = self.connection._sock.data.pop(0)
        assert cmd[0] == 'MULTI'

        # Now extract all the commands until transaction ends
        cmds_to_execute = []
        cmd = self.connection._sock.data.pop(0)
        while cmd[0] != 'EXEC':
            cmds_to_execute.append(cmd)
            cmd = self.connection._sock.data.pop(0)

        # It is a bug, if the command stack is NOT empty at this point
        assert len(self.connection._sock.data) == 0

        # execute those collected commands and construct response list
        responses = [self._parse_command_response(cmd, args) for cmd, args in cmds_to_execute]

        # Now append the expected pipeline responses to the deque and return the first response, which is 'OK'
        for i in range(len(responses)):
            self._response_queue.appendleft('QUEUED')
        self._response_queue.appendleft(responses)
        return 'OK'

    def _parse_command_response_from_connection(self, connection, type):
        cmd, args = self.connection._sock.data.pop()
        assert cmd == type
        assert len(self.connection._sock.data) == 0
        return self._parse_command_response(cmd, args)

    def _parse_command_response(self, cmd, args):
        cmd_func = getattr(self, cmd.lower())
        return cmd_func(*args)

    def pipeline(self, transaction=True):
        return FakePipelineWithStack(self, transaction)

    class _sconnection(object):
        disconnected = False

        class _socket(object):
            blocking = True
            filenos = count(30)

            def __init__(self, *args):
                self._fileno = next(self.filenos)
                self.data = []

            def fileno(self):
                return self._fileno

            def setblocking(self, blocking):
                self.blocking = blocking

        def __init__(self, client):
            self.client = client
            self._sock = self._socket()

        def disconnect(self):
            self.disconnected = True

        def send_command(self, cmd, *args):
            self._sock.data.append((cmd, args))

        def pack_commands(self, cmds):
            return cmds  # do nothing

        def send_packed_command(self, all_cmds):
            # Input command format is: tuple(tuple(cmd, arg0, arg1, ...), options)
            # The injected command format has to be equivalent to `send_command`: tuple(cmd, args)
            def normalize_command(raw_cmd):
                return (raw_cmd[0], raw_cmd[1:])
            self._sock.data.extend([normalize_command(cmd) for cmd in all_cmds])


class FakePipelineWithStack(FakePipeline):
    @property
    def command_stack(self):
        def normalize_command(raw_cmd):
            cmd, args, kwargs = raw_cmd
            return ((cmd,) + args, kwargs)

        return [normalize_command(cmd) for cmd in self.commands]
