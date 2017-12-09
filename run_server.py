import asyncio
import logging

import asyncbolt


FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)


class EchoServerSession(asyncbolt.ServerSession):
    """This is a descendant of asyncio.Protocol/asyncbolt.BoltServerProtocol"""
    def run(self, statement, parameters):
        return {'statement': statement, 'parameters': parameters}


# The rest is pretty similar to asyncio...
# Note that the first arg of create_server is a protocol class, not a factory
# it will be called with any additional kwargs passed to to create_server
loop = asyncio.get_event_loop()
coro = asyncbolt.create_server(EchoServerSession, loop=loop, host='localhost', port=8888, ssl=None)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
print('Serving on {}'.format(server.sockets[0].getsockname()))
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()