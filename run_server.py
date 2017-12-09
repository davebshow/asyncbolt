import asyncio
import logging

import asyncbolt

logging.basicConfig(level=logging.DEBUG)

class EchoServerSession(asyncbolt.ServerSession):
    """This is a descendant of asyncio.Protocol/asyncbolt.BoltServerProtocol"""
    def run(self, statement, parameters):
        return {'statement': statement, 'parameters': parameters}


# The rest is pretty similar to asyncio...
loop = asyncio.get_event_loop()
coro = asyncbolt.create_server(loop, host='localhost', port='8888', protocol_class=EchoServerSession)
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