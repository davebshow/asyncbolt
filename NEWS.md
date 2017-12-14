# News

## 0.0.2

*12/??/2-2017*

* :recycle:Added a parser that handles message transfer encoding -> simplified read buffer logic:recycle:
* :bug:Fixed bug in write buffer that allowed zero length chunks:bug: 
* :construction_worker:Updated tests to use asyncbolt.Server. Test servers now close gracefully w/o error logs.:construction_worker:

## 0.0.1

*12/09/2017*

* :boom:Initial release!:boom:


            elif self.state == ServerProtocolState.PROTOCOL_UNINITIALIZED:
                if data.signature == messaging.Message.INIT:
                    self.on_init(data.auth_token)
                    self.state = ServerProtocolState.PROTOCOL_READY
                    log_debug("Server session initialized with auth token '{}'".format(data.auth_token))
                    metadata = self.get_server_metadata()
                    self.success(metadata)
                    self.flush()
                else:
                    self.state = ServerProtocolState.PROTOCOL_FAILED
                    self.failure({})
                    self.flush()