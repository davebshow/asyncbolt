import asyncio
import asyncbolt


async def echo(loop):
    client_session = await asyncbolt.connect(loop=loop, host='localhost', port=8888)
    results = []
    async for msg in client_session.run('Hello world', {}, get_eof=True):
        results.append(msg)
    return results


loop = asyncio.get_event_loop()
results = loop.run_until_complete(echo(loop))
print(results)







