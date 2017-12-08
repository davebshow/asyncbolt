import asyncio
import logging
from asyncbolt import connect, protocol


class Neo4jBoltClientProtocol(protocol.BoltClientProtocol):

    def __init__(self, loop, *, username=None, password=None):
        super().__init__(loop)
        self.username = username
        self.password = password

    def get_init_params(self):
        return 'AsyncBolt/1.0', {"scheme": "basic", "principal": self.username, "credentials": self.password}


FORMAT = '%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

logger = logging.getLogger(__name__)
log_debug = logger.debug


async def go(loop):
    log_debug("\nSimulating the examples from the Bolt documentation...\n")
    client = await connect(
        'tcp://localhost:7687', loop,
        protocol_class=Neo4jBoltClientProtocol, username='neo4j', password='password')
    start = loop.time()
    try:
        log_debug("Running a Cypher query...\n")
        async for msg in client.run("RETURN 1 AS num", {}):
            log_debug('Client received message: \n\n{}\n'.format(msg))

        log_debug("Pipelining...\n")
        client.pipeline("RETURN 1 AS num", {})
        client.pipeline("RETURN 1 AS num", {})
        async for msg in client.run():
            log_debug('Client received message: \n\n{}\n'.format(msg))

        log_debug("Error handling with Reset...\n")
        try:
            async for msg in client.run("This will cause a syntax error", {}):
                print(msg)
        except Exception as e:
            log_debug('Client raised exception: \n\n{}\n'.format(e))

        log_debug("Accessing basic result metadata...\n")
        async for msg in client.run("CREATE ()", {}, get_eof=True):
            log_debug('Client received message: \n\n{}\n'.format(msg))

        log_debug("Explaining and profiling a query...\n")
        async for msg in client.run("EXPLAIN RETURN 1 AS num",  {}, get_eof=True):
            log_debug('Client received message: \n\n{}\n'.format(msg))

        log_debug("Accessing notifications...\n")
        async for msg in client.run("EXPLAIN MATCH (n), (m) RETURN n, m", {}, get_eof=True):
            log_debug('Client received message: \n\n{}\n'.format(msg))

        log_debug("Get somes nodes...\n")


        async for msg in client.run('MATCH (n) RETURN n'):
            log_debug('Client received message: {}'.format(msg))


        log_debug("\nReset session...\n")

        msg = await client.reset()
        log_debug('Client received message: \n\n{}\n'.format(msg))

    finally:
        print("Finished in {}".format(loop.time() - start))
        client.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(go(loop))
loop.close()
