# from http://stackoverflow.com/questions/1595492/blocks-send-input-to-python-subprocess-pipeline
from subprocess import Popen, PIPE, STDOUT
import sys, time

#p1 = Popen(["grep", "-v", "notf"], stdin=PIPE, stdout=PIPE, close_fds=True)
p1 = Popen(["./spool.py"], stdout=PIPE, close_fds=True)
#print (p1.stdin, sys.stdin)
#p2 = Popen(["cut", "-c", "1-10"], stdin=p1.stdout, stdout=PIPE, close_fds=True)
#p1.stdin.write(b'Hello World\n')
#p1.stdin.write(b"not this one\n");
#p1.stdin.flush()
#sys.stderr.write("now look in your process list for grep and cut\n")
#time.sleep(2)
#p1.stdin.close() #If I do not close, what happens?
sys.stderr.write("attempting to read from the end of the pipeline\n")
result = p1.stdout.raw.read(5)
sys.stderr.write("we read:n")
sys.stderr.write(result+"\n");
assert result == "Hello Worl\n"
