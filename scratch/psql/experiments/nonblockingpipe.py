

# FIFOs are finicky!
# I should like to be able to create and open a sink without having any sources present
# then I should like to be able to have mutliple sources send data in any order
# and also I should like to *block* when there is no data;
# 
# FIFOs, however, block at open() until a writer comes; ergo nonblocking pipes
# workaround: open with O_NONBLOCK then immediately turn it off

# there's another problem: once one source has connected and then disconnected,
# select() thinks there is data available; it's true that read() does return instead of blocking--it returns "", meaning "EOF"; but we don't care about that case, because (here's a secret): if another source appears, then read() will start giving data (not EOF! not end-of-file)
# this is a bigger problem than the awkwardness involved

# I could probably do all of this easier with a UDP socket...

import sys, os
import tempfile
import select
import fcntl


#fname = tempfile.mktemp()
fname = "/tmp/tmp9g0atw6w"
print("Creating FIFO at ", fname)
os.mkfifo(fname)

fd = os.open(fname, os.O_RDONLY) # | os.O_NONBLOCK)
pipe = os.fdopen(fd) #convert to e python object

print("Opened")

print("Enabling blocking")
fd_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
fd_flags &= ~os.O_NONBLOCK #turn off the non-blocking bit
fcntl.fcntl(fd, fcntl.F_SETFL, fd_flags)

print("Spinning")

for line in pipe:
  print(line)

print("hit EOF")
os.lseek(fd, 0, os.SEEK_SET)
while True:
  print("select says: ", select.select([fd], [fd], [fd]))
  print(os.read(fd, 101))


