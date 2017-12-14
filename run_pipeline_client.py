import asyncio
import logging

import asyncbolt


FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)


async def echo(loop):
    start_time = loop.time()
    client_session = await asyncbolt.connect(loop=loop, host='localhost', port=8888, max_inflight=2048)
    for _ in range(1000):
        client_session.pipeline('Hello world', {})
    async for msg in client_session.run():
        print(msg)
    client_session.close()
    print("\nPipelined 1000 messages in {} seconds".format(loop.time() - start_time))


loop = asyncio.get_event_loop()
loop.run_until_complete(echo(loop))