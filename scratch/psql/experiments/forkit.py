
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
# I know what's the problem:
#  this is being hard-killed (-9'd, or something), and so the process simply dies without having a change to clean up after itself.
# The way I've written the code, it only calls Changes.__exit__() (--> so therefore calls unregister())
#  if an exception happens *within* replicate--and not just that,
#  but within the thread doing the spooling.
#
# ...so how do I fix this?
# I could trap the exit signal maybe, but that seems like a patchwork solution and won't run on the right thread
# I could have a monitor thread watching (via select(), even) sys.stdin or sys.stdout and looking for EOF --- I've got this written and it reliably catches the quit--and then raise a signal to trigger the main loop to quit
#   ^ this is flakey, because there's no guarantee this thread will run between the socket closing and the kill coming
# I could restructure the code so that the spooling loop select()s on *both* the input (unix domain socket from inside of postgres) and output (stdout) streams. Then, when 
# WORKING BUT AWKWARD SOLUTION: set end-close on the EXEC in socat; this makes socat forgo the; then, do the restructuring to watch for stdin falling over.
#  CURRENTLY AWKWARD because I've written
#     for delta in replicate(table):
#        if select.select([sys.stdin], [], [],0)[0]:
#  so replicants don't find out they should die until the next time a delta comes from the database. This isn't the end of the world, but it certainly provides a way (for an attacker?) to chew up resources if we also allow clients to indirectly control writes to the DB: open and close the client page a million times; this will spawn a million replicates which won't ever close
#   I think the only other ways to detect the client dying are
#    - rewrite as a socket app and check explicitly
#    - use signal
# the monitoring thread is essentially no better than restructuring the code; the code will *still* have to be 
# # Oh! If it is important that we quit, then make the main thread the one monitoring stdin
# and put
# hm but really 
# ..oh, except.. well... we're using datagrams on the input side so we don't have...

# This is additionally made extra complicated by replicate.sh, since processes do not take their children with them by default
# --> it would be better if replicate.sh could be avoided -- all it does is set an environment var, and then only on OS X

while True:
	time.sleep(1)
	

