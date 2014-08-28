#!/usr/bin/env python3
#brutally forced attempt to get postgres to send the WAL logs;
# this below gets as far as turning on the correct mode and sending the sub-protocol header line.
# this is python3!

import socket, struct

ss = socket.socket()
ss.connect(("localhost", 5432))
TYPECODE = b""
VERSION = struct.pack("!I", 196608)
#OPTIONS = "user\0postgres\0".enucode("ascii")
OPTIONS = {"user": "postgres", "application_name": "crack this mother open", "replication": "true"} #experimentally, replication is a boolean option, which means it can be ascii "true", "false", "0" or 
"1"
# note: you must edit pg_hba.conf to allow replication
OPTIONS = str.join("", ("%s\x00%s\x00" % e for e in OPTIONS.items())).encode("ascii")
TERMINATOR = b"\x00" #like HTTP, postgres requires a doubleending to end the headers
PAYLOAD = VERSION + OPTIONS + TERMINATOR;

SIZE = struct.pack("!I", 4 + len(PAYLOAD))
MESSAGE = TYPECODE + SIZE + PAYLOAD
print(MESSAGE)
ss.send(MESSAGE)

TYPECODE = b"Q"
PAYLOAD = "IDENTIFY_SYSTEM\x00".encode("ascii")
SIZE = struct.pack("!I", 4 + len(PAYLOAD))
MESSAGE = TYPECODE + SIZE + PAYLOAD
print(MESSAGE)
ss.send(MESSAGE)





M = ss.recv(111)
while M:
	print(M, flush=True)
	M = ss.recv(111)

import time; time.sleep(10)
