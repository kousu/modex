

# How do I make, name and listen to datagram unix sockets in python?
# 

from socket import *

import tempfile

sock = socket(AF_UNIX, SOCK_DGRAM)

fname = tempfile.mktemp() #XXX race condition security hole
sock.bind(fname) #creates the socket file


from select import *

while True:
  ss = select([sock], [], [sock])
  
  print("select says ", ss)
  msg = sock.recv(2<<16)
  print("received |%s|" % (msg,))