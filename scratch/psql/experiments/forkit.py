
# socat with the "fork" option holds open programs even after they've died
# maybe I need to check is sys.stdout exists????

import sys
import uuid

import threading, time
import atexit

I = uuid.uuid4()
i = 0

def alive():
	global i
	while True:
		#a = input()
		#print(a*2)
		sys.stderr.write(str(I)+":"+str(i))
		sys.stderr.write("\n")
		sys.stderr.flush()
		time.sleep(2)
		i += 1

T = threading.Thread(target=alive);
T.start()

def q():
	print("quittign")
	sys.stderr.write("stderr::quitting\n"); sys.stderr.flush()
atexit.register(q)

# used with socat's EXEC address, e.g.
# socat UNIX-LISTEN:/tmp/sw,fork,reuseaddr EXEC:"python forkit.py"
# socat TCP-LISTEN:7777,fork,reuseaddr EXEC:"python forkit.py"
# and then connected to
# this is indeed shutdown properly by socat when the client disconnects.
# However, for some reason replicate.py doesn't shutdown.
# 

while True:
	time.sleep(1)
	

