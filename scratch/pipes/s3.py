#!/usr/bin/env python3
 
import os
import sys
from subprocess import Popen, PIPE
 
def produce(to_sed):
     for i in ('beet', 'meet', 'sneet'):
         to_sed.write(i + '\n')
         to_sed.flush()
 
def consume(from_sed):
    print "CONSUME"; sys.stdout.flush()
    while 1:
        print "TRYING TO READ from_sed"; sys.stdout.flush()
        res = from_sed.readline() 
        print "READ from_sed"; sys.stdout.flush()
        if not res:
            sys.exit(0)
            #sys.exit(proc.poll())
        print 'received: ', [res]


def main():
    #proc = ['sed', 's/ee/oo/g' ]
    proc = ["./spool.py"]
    log = open("spool.err","wb",buffering=0)
    proc = Popen(proc,stdin=PIPE,stdout=PIPE,stderr=log)
    to_sed = proc.stdin
    from_sed = proc.stdout

    pid = os.fork()
    if pid == 0:
        from_sed.close()
        produce(to_sed)
        return
    else:
        to_sed.close()
        consume(from_sed)

if __name__ == '__main__':
    main()
