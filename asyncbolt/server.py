import asyncio
import collections
import logging

from asyncbolt import protocol


logger = logging.getLogger(__name__)
log_debug = logger.debug
log_info = logger.info
log_warning = logger.warning
log_error = logger.error


class ServerSession(protocol.BoltServerProtocol):
    """Implement the Bolt protocol for server"""

    def __init__(self, loop, *, server=None):
        super().__init__(loop)
        self.server = server
        self.queue = asyncio.Queue()
        self.task_queue_handler = self.loop.create_task(self.run_task_queue())
        self.active_run_task = None
        self.waiters = collections.deque()
        self.waiters_append = self.waiters.append
        self.waiters_popleft = self.waiters.popleft
        self.close_handler = None

    def close(self):
        self.state = protocol.ServerProtocolState.PROTOCOL_CLOSED
        if self.active_run_task:
            self.active_run_task.cancel()
        self.task_queue_handler.cancel()
        self.transport = None
        self.close_handler = self.loop.create_task(self.wait_closed())

    async def wait_closed(self):
        if self.active_run_task:
            try:
                await self.active_run_task
            except asyncio.CancelledError:
                pass
        try:
            await self.task_queue_handler
        except asyncio.CancelledError:
            pass
        self.state = protocol.ServerProtocolState.PROTOCOL_CLOSED

    def restart_task_queue(self):
        if self.task_queue_handler:
            self.task_queue_handler.cancel()
        self.task_queue_handler = self.loop.create_task(self.run_task_queue())

    async def run_task_queue(self):
        while True:
            try:
                active_run_task, future = await self.queue.get()
                self.active_run_task = self.loop.create_task(active_run_task)
                fields = await self.active_run_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.state = protocol.ServerProtocolState.PROTOCOL_FAILED
                self.failure({})
                try:
                    await future
                    self.ignored({})
                except:
                    pass  # look into this
                self.flush()
            else:
                self.success({})
                self.record([fields])
                self.success({})
                await future  # Pull All is called, flush queue...
                self.flush()
                self.active_run_task = None
                log_debug("Packed fields '{}'".format(fields))

    def discard_run_task(self, *_):
        self.active_run_task = None

    def on_ack_failure(self):
        if self.active_run_task:
            self.active_run_task.cancel()
        self.restart_task_queue()

    def on_discard_all(self):
        if self.active_run_task:
            self.active_run_task.cancel()

    def on_pull_all(self):
        waiter = self.waiters_popleft()
        waiter.set_result(True)

    def on_reset(self):
        if self.active_run_task:
            self.active_run_task.cancel()
        self.restart_task_queue()

    def on_run(self, data):
        future = asyncio.Future(loop=self.loop)
        self.waiters_append(future)
        self.queue.put_nowait((self.run(data), future))

    async def run(self, data):
        """Inheriting server protocol must implement this method."""
        log_warning("""Server received run message {}
                       Inheriting classes must implement `run`""".format(data))
        raise NotImplementedError


class Server:
    """Protocol factory class that allows for appropriate shutdown of cons on close"""
    def __init__(self, loop, protocol_class, **kwargs):
        self._loop = loop
        self._protocol_class = protocol_class
        self._kwargs = kwargs
        self._connections = {}
