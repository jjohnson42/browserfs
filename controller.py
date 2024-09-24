#!/usr/bin/python3

import socket
import sys
import struct

a = socket.socket(socket.AF_UNIX)
a.connect(sys.argv[1])
subname = sys.argv[2].encode()
a.send(struct.pack('!II', 1, len(subname)))
a.send(subname)
fname = sys.argv[3].encode()
a.send(struct.pack('!I', len(fname)))
a.send(fname)
rsp = a.recv(4)
retcode = struct.unpack('!I', rsp)[0]
if retcode != 0:
    raise Exception("Bad return code")
rsp = a.recv(4)
nlen = struct.unpack('!I', rsp)[0]
idstr = a.recv(nlen).decode('utf8')
rsp = a.recv(4)
nlen = struct.unpack('!I', rsp)[0]
authtok = a.recv(nlen).decode('utf8')
print(f"./target/debug/websocat ws://localhost:8080/{idstr}/{sys.argv[2]}/{sys.argv[3]}\n with authtok {authtok}")
