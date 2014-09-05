#! /bin/env python

import os
import sys
import signal
from subprocess import Popen, PIPE
import multiprocessing
from Queue import Empty

def produce(to_processor,pending):
    while 1:
        try:
            item = pending.get(False)
        except Empty:
            item = ''
        if item is produce:
            #that's the signal to stop!
            to_processor.close()
            return
        #must keep filling the buffer with something ('\n')
        to_processor.write(item + '\n')
        to_processor.flush()

def consume(from_processor,done):
    while 1:
        res = from_processor.readline() 
        print("READ THIS:", res)
        if not res:
            from_processor.close()
            done.put(consume)
            return
        done.put(res)

def controller(pending, done):
    for i in ('beet', 'meet', 'sneet'):
        pending.put(i)
    needed = 3
    quantity_done = 0
    while 1:
        item = done.get()
        if item and item != '\n':
            if item.startswith('b'):
                pending.put('Z' + item)
                needed += 1
            quantity_done +=1
            print item,
        if quantity_done == needed:
            pending.put(produce)
            break
    while 1:
        item = done.get()
        if item is consume:
            return

def main():
    r'''
    workflow:
        producer  ->   processor ->   consumer
            ^                            /
             \                          v 
            pending                  done
                   ^                   /
                    \                 v
                        controller
    components:

        controller:  the original script
        producer:  a forked clone of controller
        consumer:  a forked clone of controller
        processor:  a subprocess.popen instance
        pending:  a multiprocessing queue
        done:  a multiprocessing queue
    '''
    #proc = ['sed', 's/ee/oo/g' ]
    proc = ["./spool.py"]
    proc = Popen(proc,stdin=PIPE,stdout=PIPE)
    to_processor, from_processor = proc.stdin, proc.stdout

    pending = multiprocessing.Queue()
    pid = os.fork()
    if pid == 0:
        from_processor.close()
        produce(to_processor,pending)
        return
    done = multiprocessing.Queue()
    pid2 = os.fork()
    if pid2 == 0:
        to_processor.close()
        consume(from_processor,done)
        return
    to_processor.close()
    from_processor.close()
    res = controller(pending, done)
    os.waitpid(pid,0)
    os.waitpid(pid2,0)
    return res


if __name__ == '__main__':
    main()
