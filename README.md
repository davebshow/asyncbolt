# asyncbolt

:zap:`asyncbolt`:zap: is an implementation of the Neo4j [Bolt](https://boltprotocol.org/) client/server protocol for Python [Asyncio](https://docs.python.org/3/library/asyncio.html).

The `asyncbolt.session.ClientSession` object aims to be fully compatible with Neo4j (or any other server that
speaks Bolt), but it won't provide a fully featured Neo4j client.

- Python >= 3.6
- 0 dependencies

## Example use

Install with pip:

```buildoutcfg
$ comingsoon
```

### Client

#### Basics

Using the client is easy. The following example uses Neo4j.

Get the server, unpack, and cd:

```buildoutcfg
$ wget dist.neo4j.org/neo4j-community-1.9.9-unix.tar.gz
$ tar xvf neo4j-community-3.3.1-unix.tar.gz
$ cd neo4j-community-3.3.1/
```

Fire it up:
```buildoutcfg
$ bin/neo4j start
```

Typically, you will want to use some kind of authorization with the server. `asyncbolt.BoltClientProtocol` doesn't
support authentication out of the box, but it is easy to create a subclass that does:

```python
import asyncio
import asyncbolt


class Neo4jBoltClientProtocol(asyncbolt.BoltClientProtocol):

    def __init__(self, loop, *, username=None, password=None):
        super().__init__(loop)
        self.username = username
        self.password = password

    def get_init_params(self):
        return 'AsyncBolt/1.0', {"scheme": "basic", "principal": self.username, "credentials": self.password}
```

The methods `__init__` and `get_init_params` are basically the only methods inheriting protocols need implement, as
authorization will typically be the only difference between server implementations, at least from the client's perspective.

Use `asyncbolt.connect` to create an `asyncbolt.ClientSession` instance, passing the custom protocol class, and its
kwargs:

```python
loop = asyncio.get_event_loop()

client = await connect('tcp://localhost:7687', loop,
                       protocol_class=Neo4jBoltClientProtocol,
                       username='neo4j', password='password')
    
async for msg in client.run("RETURN 1 AS num", {}):
    print(msg)
# ClientResponse(fields=[1], metadata={'result_available_after': 0, 'fields': ['num']}, eof=False)
```

If you are interested in extra metadata sent by the Neo4j server be sure to set the `get_eof` kwarg to `True` when
calling the `run` method. For example, when you want to use query profiling/explanation:

```python
async for msg in client.run("EXPLAIN RETURN 1 AS num",  {}, get_eof=True):
    print(msg)
#ClientResponse(
#    fields=None,
#    metadata={'result_consumed_after': 0,
#              'type': 'r',
#              'plan': {'args': {'runtime': 'INTERPRETED',
#                                'planner-impl': 'IDP',
#                                'runtime-impl': 'INTERPRETED',
#                                'version': 'CYPHER 3.3',
#                                'EstimatedRows': 1.0,
#                                'planner': 'COST'}, 
#                      'children': [
#                        {'args': {'EstimatedRows': 1.0, 'Expressions': '{num : {  AUTOINT0}}'},
#                         'children': [], 'identifiers': ['num'], 'operatorType': 'Projection'}],
#                      'identifiers': ['num'], 'operatorType': 'ProduceResults'}},
#    eof=True)
```

The `asyncbolt.ClientSession` object appears to communicate fluently with the Neo4j Server. The script `bolt_neo4_demo.py`
implements some of the examples from the Bolt protocol homepage. It can be run as follows.

Clone this repo, cd, and run:
```buildoutcfg
$ git clone
$ cd asyncbolt
$ python bolt_neo4j_demo.py
```

This script sets logging to debug and produces the following output:

```txt
Simulating the examples from the Bolt documentation...

Connection made to: ('127.0.0.1', 7687)
Sending handshake with version info: b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
Using Bolt protocol version b'\x00\x00\x00\x01'

Writing message to transport
'b'\x00C\xb2\x01\x8dAsyncBolt/1.0\xa3\x86scheme\x85basic\x89principal\x85neo4j\x8bcredentials\x88password\x00\x00''

Data received:
b'\x00\x16\xb1p\xa1\x86server\x8bNeo4j/3.3.1\x00\x00'

Client session initialized with server metadata:

{'server': 'Neo4j/3.3.1'}

Running a Cypher query...

Pipelining statement and params:
RETURN 1 AS num
{}

Pipelining PULL_ALL

Writing message to transport
'b'\x00\x13\xb2\x10\x8fRETURN 1 AS num\xa0\x00\x00\x00\x02\xb0?\x00\x00''

Data received:
b'\x00(\xb1p\xa2\xd0\x16result_available_after\x00\x86fields\x91\x83num\x00\x00\x00\x04\xb1q\x91\x01\x00\x00\x00"\xb1p\xa2\xd0\x15result_consumed_after\x00\x84type\x81r\x00\x00'

Client received message: 

ClientResponse(fields=[1], metadata={'result_available_after': 0, 'fields': ['num']}, eof=False)

Pipelining...

Pipelining statement and params:
RETURN 1 AS num
{}

Pipelining PULL_ALL

Pipelining statement and params:
RETURN 1 AS num
{}

Pipelining PULL_ALL

Writing message to transport
'b'\x00\x13\xb2\x10\x8fRETURN 1 AS num\xa0\x00\x00\x00\x02\xb0?\x00\x00\x00\x13\xb2\x10\x8fRETURN 1 AS num\xa0\x00\x00\x00\x02\xb0?\x00\x00''

Data received:
b'\x00(\xb1p\xa2\xd0\x16result_available_after\x00\x86fields\x91\x83num\x00\x00\x00\x04\xb1q\x91\x01\x00\x00\x00"\xb1p\xa2\xd0\x15result_consumed_after\x00\x84type\x81r\x00\x00\x00(\xb1p\xa2\xd0\x16result_available_after\x00\x86fields\x91\x83num\x00\x00\x00\x04\xb1q\x91\x01\x00\x00\x00"\xb1p\xa2\xd0\x15result_consumed_after\x00\x84type\x81r\x00\x00'

Client received message: 

ClientResponse(fields=[1], metadata={'result_available_after': 0, 'fields': ['num']}, eof=False)

Client received message: 

ClientResponse(fields=[1], metadata={'result_available_after': 0, 'fields': ['num']}, eof=False)

Error handling with Reset...

Pipelining statement and params:
This will cause a syntax error
{}

Pipelining PULL_ALL

Writing message to transport
'b'\x00#\xb2\x10\xd0\x1eThis will cause a syntax error\xa0\x00\x00\x00\x02\xb0?\x00\x00''

Data received:
b'\x00\x9e\xb1\x7f\xa2\x84code\xd0%Neo.ClientError.Statement.SyntaxError\x87message\xd0eInvalid input \'T\': expected <init> (line 1, column 1 (offset: 0))\n"This will cause a syntax error"\n ^\x00\x00\x00\x02\xb0~\x00\x00'

Writing message to transport
'b'\x00\x02\xb0\x0f\x00\x00''

Data received:
b'\x00\x03\xb1p\xa0\x00\x00'

Client raised exception: 

Server failed. Reset with '15'

Accessing basic result metadata...

Pipelining statement and params:
CREATE ()
{}

Pipelining PULL_ALL

Writing message to transport
'b'\x00\r\xb2\x10\x89CREATE ()\xa0\x00\x00\x00\x02\xb0?\x00\x00''

Data received:
b'\x00$\xb1p\xa2\xd0\x16result_available_after\x01\x86fields\x90\x00\x00\x008\xb1p\xa3\x85stats\xa1\x8dnodes-created\x01\xd0\x15result_consumed_after\x00\x84type\x81w\x00\x00'

Client received message: 

ClientResponse(fields=None, metadata={'stats': {'nodes-created': 1}, 'result_consumed_after': 0, 'type': 'w'}, eof=True)

Explaining and profiling a query...

Pipelining statement and params:
EXPLAIN RETURN 1 AS num
{}

Pipelining PULL_ALL

Writing message to transport
'b'\x00\x1c\xb2\x10\xd0\x17EXPLAIN RETURN 1 AS num\xa0\x00\x00\x00\x02\xb0?\x00\x00''

Data received:
b'\x00(\xb1p\xa2\xd0\x16result_available_after\x00\x86fields\x91\x83num\x00\x00\x01M\xb1p\xa3\xd0\x15result_consumed_after\x00\x84type\x81r\x84plan\xa4\x84args\xa6\x87runtime\x8bINTERPRETED\x8cplanner-impl\x83IDP\x8cruntime-impl\x8bINTERPRETED\x87version\x8aCYPHER 3.3\x8dEstimatedRows\xc1?\xf0\x00\x00\x00\x00\x00\x00\x87planner\x84COST\x88children\x91\xa4\x84args\xa2\x8dEstimatedRows\xc1?\xf0\x00\x00\x00\x00\x00\x00\x8bExpressions\xd0\x14{num : {  AUTOINT0}}\x88children\x90\x8bidentifiers\x91\x83num\x8coperatorType\x8aProjection\x8bidentifiers\x91\x83num\x8coperatorType\x8eProduceResults\x00\x00'

Client received message: 

ClientResponse(fields=None, metadata={'result_consumed_after': 0, 'type': 'r', 'plan': {'args': {'runtime': 'INTERPRETED', 'planner-impl': 'IDP', 'runtime-impl': 'INTERPRETED', 'version': 'CYPHER 3.3', 'EstimatedRows': 1.0, 'planner': 'COST'}, 'children': [{'args': {'EstimatedRows': 1.0, 'Expressions': '{num : {  AUTOINT0}}'}, 'children': [], 'identifiers': ['num'], 'operatorType': 'Projection'}], 'identifiers': ['num'], 'operatorType': 'ProduceResults'}}, eof=True)

Accessing notifications...

Pipelining statement and params:
EXPLAIN MATCH (n), (m) RETURN n, m
{}

Pipelining PULL_ALL

Writing message to transport
'b'\x00\'\xb2\x10\xd0"EXPLAIN MATCH (n), (m) RETURN n, m\xa0\x00\x00\x00\x02\xb0?\x00\x00''

Data received:
b'\x00(\xb1p\xa2\xd0\x16result_available_after\x00\x86fields\x92\x81n\x81m\x00\x00\x04X\xb1p\xa4\xd0\x15result_consumed_after\x00\x84type\x81r\x84plan\xa4\x84args\xa6\x87runtime\x8bINTERPRETED\x8cplanner-impl\x83IDP\x8cruntime-impl\x8bINTERPRETED\x87version\x8aCYPHER 3.3\x8dEstimatedRows\xc1?\xf0\x00\x00\x00\x00\x00\x00\x87planner\x84COST\x88children\x91\xa4\x84args\xa1\x8dEstimatedRows\xc1?\xf0\x00\x00\x00\x00\x00\x00\x88children\x92\xa4\x84args\xa1\x8dEstimatedRows\xc1?\xf0\x00\x00\x00\x00\x00\x00\x88children\x90\x8bidentifiers\x91\x81n\x8coperatorType\x8cAllNodesScan\xa4\x84args\xa1\x8dEstimatedRows\xc1?\xf0\x00\x00\x00\x00\x00\x00\x88children\x90\x8bidentifiers\x91\x81m\x8coperatorType\x8cAllNodesScan\x8bidentifiers\x92\x81m\x81n\x8coperatorType\xd0\x10CartesianProduct\x8bidentifiers\x92\x81m\x81n\x8coperatorType\x8eProduceResults\x8dnotifications\x91\xa5\x88severity\x87WARNING\x8bdescription\xd1\x01\xa9If a part of a query contains multiple disconnected patterns, this will build a cartesian product between all those parts. This may produce a large amount of data and slow down query processing. While occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (identifier is: (m))\x84code\xd08Neo.ClientNotification.Statement.CartesianProductWarning\x88position\xa3\x86offset\x08\x86column\t\x84line\x01\x85title\xd0DThis query builds a cartesian product between disconnected patterns.\x00\x00'

Client received message: 

ClientResponse(fields=None, metadata={'result_consumed_after': 0, 'type': 'r', 'plan': {'args': {'runtime': 'INTERPRETED', 'planner-impl': 'IDP', 'runtime-impl': 'INTERPRETED', 'version': 'CYPHER 3.3', 'EstimatedRows': 1.0, 'planner': 'COST'}, 'children': [{'args': {'EstimatedRows': 1.0}, 'children': [{'args': {'EstimatedRows': 1.0}, 'children': [], 'identifiers': ['n'], 'operatorType': 'AllNodesScan'}, {'args': {'EstimatedRows': 1.0}, 'children': [], 'identifiers': ['m'], 'operatorType': 'AllNodesScan'}], 'identifiers': ['m', 'n'], 'operatorType': 'CartesianProduct'}], 'identifiers': ['m', 'n'], 'operatorType': 'ProduceResults'}, 'notifications': [{'severity': 'WARNING', 'description': 'If a part of a query contains multiple disconnected patterns, this will build a cartesian product between all those parts. This may produce a large amount of data and slow down query processing. While occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (identifier is: (m))', 'code': 'Neo.ClientNotification.Statement.CartesianProductWarning', 'position': {'offset': 8, 'column': 9, 'line': 1}, 'title': 'This query builds a cartesian product between disconnected patterns.'}]}, eof=True)

Get somes nodes...

Pipelining statement and params:
MATCH (n) RETURN n
{}

Pipelining PULL_ALL

Writing message to transport
'b'\x00\x17\xb2\x10\xd0\x12MATCH (n) RETURN n\xa0\x00\x00\x00\x02\xb0?\x00\x00''

Data received:
b'\x00&\xb1p\xa2\xd0\x16result_available_after\x00\x86fields\x91\x81n\x00\x00\x00\x08\xb1q\x91\xb3N\x00\x90\xa0\x00\x00\x00\x08\xb1q\x91\xb3N\x01\x90\xa0\x00\x00\x00\x08\xb1q\x91\xb3N\x02\x90\xa0\x00\x00\x00\x08\xb1q\x91\xb3N\x03\x90\xa0\x00\x00\x00\x08\xb1q\x91\xb3N\x04\x90\xa0\x00\x00\x00"\xb1p\xa2\xd0\x15result_consumed_after\x00\x84type\x81r\x00\x00'

Client received message: ClientResponse(fields=[Node(signature=78, nodeIdentity=0, labels=[], properties={})], metadata={'result_available_after': 0, 'fields': ['n']}, eof=False)
Client received message: ClientResponse(fields=[Node(signature=78, nodeIdentity=1, labels=[], properties={})], metadata={'result_available_after': 0, 'fields': ['n']}, eof=False)
Client received message: ClientResponse(fields=[Node(signature=78, nodeIdentity=2, labels=[], properties={})], metadata={'result_available_after': 0, 'fields': ['n']}, eof=False)
Client received message: ClientResponse(fields=[Node(signature=78, nodeIdentity=3, labels=[], properties={})], metadata={'result_available_after': 0, 'fields': ['n']}, eof=False)
Client received message: ClientResponse(fields=[Node(signature=78, nodeIdentity=4, labels=[], properties={})], metadata={'result_available_after': 0, 'fields': ['n']}, eof=False)

Reset session...

Writing message to transport
'b'\x00\x02\xb0\x0f\x00\x00''

Data received:
b'\x00\x03\xb1p\xa0\x00\x00'

Client received message: 

ClientResponse(fields=['Successfully reset sever session'], metadata={}, eof=True)

Finished in 0.018699501997616608

```

#### Pipelining
As you can see, `asyncbolt` using pipelining by default. Users can also choose to pipeline multiple message together.
Done properly, this can result in huge performance gains.

#### Reset

#### Custom Client Protocols (subclassing `asyncbolt.BoltClientProtocol`)

## Server

### Subclassing `asyncbolt.ServerSession`

### Subclassing `asyncbolt.BoltServerProtocol`



## TODOs
'asyncbolt' needs a lot of work. Contributions are welcome.

* Tests, tests, tests (Neo4j, *buffers*, etc., etc.)
* Improve buffer implementation/testing
* Profiling and optimization (Cython/C extensions)
* CI
* Docs...? The idea is that need for documentation is minimal...
* Much, much, more...

*Notes*

- `asyncbolt` does provide decoders for Graph Type structures, as these are expected in Record messages sent by the Neo4j
server. It does not provide message packing for graph type structures. In the future, these may be added, or
`asyncbolt.messaging.ENCODERS` may be extended in application code.


