# -*- coding: utf-8 -*-
# from http://www.velvetcache.org/2010/06/14/python-unix-sockets

import sys

import socket
import os, os.path
import time

addr = sys.argv[1]

if os.path.exists( addr ):
  os.remove( addr )
 
print ("Opening socket", addr)
server = socket.socket( socket.AF_UNIX, socket.SOCK_DGRAM )
server.bind(addr)
 
print("Listening...")
while True:
  datagram = server.recv( 1024 )
  datagram = datagram.decode("utf-8") #hurp a durp py2 hackaroudn
  if not datagram:
    break
  else:
    print("-" * 20)
    print(datagram)
    if "DONE" == datagram:
      break
print("-" * 20)
print("Shutting down...")
server.close()
os.remove( addr )
print("Done")
