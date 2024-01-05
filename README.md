Goal is to facilitate media import from file on client device.

Closest match was "webfuse", but not designed with web browser client in mind, and overkill.  Too expansive a protocol, would require a lot of javascript work.

Currently looking at bazil fuse in go for filesystem side, and gorilla websocket.

Notion is two sockets:
-Web Socket for reverse proxy
-Control socket for registering/unregistering files

Web Socket:
-Connect to url
-First data from client is file publish token
-Read requests from fuse forwarded over websocket

Control socket:
-Connect to unix domain socket
-Open with control registration token (should be redundant with file permissions)
-Register file, request:
    -pathname (user/file.ext)
 response:
    -full pathname (token/user/file.ext)
    -file publish token
-Close file, request is full pathname
