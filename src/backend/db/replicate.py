#!/usr/bin/env python
# usage: replicate table --> stdout posts a line-oriented stream of HRDJ
# third-time's-a-charm edition
# depends: sqlalchemy

# depends: watch.pysql and replicate.pysql are loaded into postgres

"""
TODO:
 irritant: select() blocks ctrl-c, because it's an I/O wait (quick fix: give a long timeout on select and spin it in a polling loop)

clean up the sql injections

clean up naming ("my_pg"? really?)

support views (filtered replication) ---- doing this right requires some finesse. I have the logic written down in replicant.py, but distinguishing each client from each other will be tricky.

consider whether we can ditch SQLAlchemy; all this script does is issue simple SQL commands.
 speaking DBAPI directly would be less heavy
 

unregister is not getting called on ctrl-c or other exceptions, even though I've
 --> since the with: is inside of a generator,there's paths through the code which quit without going through the .__exit__(). Very annoying.
 ...but it should mostly be blocking in Changes.__next__, so why is this such an obvious problem??

Concurrency bugs:
 by adding artificial stalls (time.sleep()) to the script, you can demonstrate these to yourself
 1) writes that occur after "cur = " but before "changes = " go to the great bit bucket in the sky
   solutions:
    a. figure out some way to ask postgres for timeline location id (xid)s and ask it to replay from those certain poitns
    b. Use an explicit table lock around acquiring the two cursors (NB: the changes cursor is not a SQL cursor)
      - http://www.postgresql.org/docs/current/static/sql-lock.html / http://www.postgresql.org/docs/current/static/explicit-locking.html
 ...i think that's the only one. We could get the changes cursor first, and then be in the equally difficult situation of having double-writes (so, a delete would show up as a nonexistent row and then a delete which would get confused, an insert would show up as a duplicated row, an update would show up as both -- or maybe it would show up as unable to be applied since the old row would). If a human was doing this work, these sorts of errors would be manageable, but for a computer this is equally difficult.
  but since we make sure to (let the kernel) buffer changes for us simultaneously to the current state being written out, we should at least not miss anything in the gaps of writing out the current state.
 
"""

import sys
import os, tempfile
import socket, select
import signal, threading

import sqlalchemy, json


import logging
logging.getLogger().setLevel(logging.DEBUG)


if os.uname().sysname == "Darwin":
    # there's deployment issues on OS X:
    # http://stackoverflow.com/questions/16407995/psycopg2-image-not-found
    # this is an unfinished hack to get around it which might not even work
    os.environ["DYLD_LIBRARY_PATH"] = "/Applications/Postgres.app/Contents/Versions/9.3/lib/"
    #+ ":" + os.environ.setdefault("DYLD_LIBRARY_PATH","")
    try:
        import psycopg2
    except ImportError:
        sys.stderr.write("Unable to import psycopg2 under OS X. Do you have Postgres.app installed? Do you have DYLD_FALLBACK_LIBRARY_PATH set to point at it?")
        # TODO: print traceback here
        sys.exit(3)




def shutdown():
    """
    Tell all the spools in this program that are blocked on select() to stop.
    
    This should be triggered by
     SIGINT
     SIGQUIT
     SIGTERM
     and by stdin or stdout closing
    but not by
      SIGKILL (which we cannot catch anyway)
      SIGSTOP (which we cannot catch anyway)
      SIGABRT (which we can catch but means roughly the same as SIGKILL: "die without cleaning up")
    and not by stderr
    TODO: write tests which cover all these cases
    """
    global shutdown_socket
    shutdown_socket.close()
    
def shutdown_on_signal(SIG, stack_frame):
    shutdown()
    

def alivethread():
    """
    poll stdin for EOF, and signal shutdown when that occurs
    """
    while True:
        select.select([sys.stdin],[],[]) #block
        if sys.stdin.read(1) == "": #EOF
            break
    shutdown()




class Changes:
    MTU = 2048 #maximum bytes to read per message
    
    def __init__(self, table): #TODO: support where clauses
      self._table = table
      self._stream_id = None
      
    def __enter__(self):
      # set up our listening socket
      self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
      self._sock.bind(tempfile.mktemp(prefix="pg_replicate_%s" % (self._table,))) #XXX RACE CONDITION; mktemp.__doc__ specifically says "this function is unsafe"; however, how else to make a socket?
        # also using the table name in the file name is probably poor form
      logging.info("listening for changes to %s on %s", self._table, self._sock.getsockname())
      
      # register ourselves with the source
      # XXX SQL injection here
      # autocommit is explicitly turned on because SQLAlchemy assumes all selects are mutationless
      # the docs expplicitly cover how to workaround this: http://docs.sqlalchemy.org/en/rel_0_9/core/connections.html#understanding-autocommit
      C = E.connect()
      r = C.execution_options(autocommit=True).execute("select * from my_pg_replicate_register('%s', '%s')" % (self._table, self._sock.getsockname()))
      r = r.scalar()
      logging.debug("register() result is: %s", r)
      C.close()
      
      self._stream_id = r
      
      return self #oops!
    
    def __exit__(self, *args):
      
      # unregister ourselves
      # XXX SQL injection here
      C = E.connect()
      r = C.execution_options(autocommit=True).execute("select * from my_pg_replicate_unregister('%s')" % (self._stream_id))
      r = r.scalar()
      logging.debug("unregister() result is: %s", r)
      C.close()
      
      # shutdown the socket
      os.unlink(self._sock.getsockname()) # necessary because we're using unix domain sockets
      self._sock.close()
    
    def __iter__(self):
      return self
    
    def __next__(self):
      fread, fwrite, ferr = select.select([self._sock, shutdown_listener], [], [self._sock, shutdown_listener])  #block until some data is available
      if ferr:
        pass #XXX what should we do in this case?
      else:
        if shutdown_listener in fread: #check for EOF from shutdown_socket
            raise StopIteration
        pkt = self._sock.recv(Changes.MTU)
        pkt = pkt.decode('utf-8')
        return pkt
    
    next = __next__ #py2 compatibility
  

def replicate(table):
    # XXX we might need to construct the Changes stream first
    #  If we do have concurrency problems, doing that at least guarantees that we don't miss any changes, though we might end up with duplicate rows or trying to delete nonexistent rows

    # 2) get a handle on the change stream beginning at the commit postgres was at *now* (?? maybe this involves locking?)
    #  XXX we're relying on the time between the select and Changes.__enter__() to be small enough to be atomically
    #   but that's never absolutely true so this has a race condition!
    #  It would be better if we could ask postgres
    #   "what is the lamport clock of our previous select", which postgres internally records [CITE: ....]
    #  And then say "with Changes(_table,[ columns,][ where,] from=clock)"
    # (NB: a lamport clock is a count of events; it has nothing to do with real time except that it always increases in time, making it suitable for synchronizing concurrent processes even when their system clocks might skew)
    # but postgres doesn't seem(?) to provide a way to extract; it just uses timelines to make sure every session sees a consistent set of data.
    # this is sort of tricky
    # I need to say somethign like
  
    with Changes(table) as changes: #<-- use with to get the benefits of RAII, since Changes has a listening endpoint to worry about cleaning up

        # README: BUGFIX: the change to raw_connection() caused a deadlock which only occurs the first time register() is called: register() needs to create a trigger on _table, but cur holds a lock on _table
        #  it seems, however, that reordering the instructions avoids the deadlock
        #  and i was already considering doing this; this order means we potentially have overlapping state in the Changes and cur feeds
        
        # 1) get a cursor on the current query

        # XXX question: are we allowed to have multiple results open during a single connection? Does this cause deadlocks? This program only uses *one* result set: the Changes feed is not coming from a resultset, it's listening to a socket.

        # get current state with SQLAlchemy #XXX commented out because this was causing a deadlock; TODO: revisit and see if you can avoid using raw_connection()
        # stream_results is turned on for this query so that this line takes as little time as possible
        #cur = C.execution_options(stream_results=False).execute("select * from %s" % (_table,))
        

        C_DBAPI = E.raw_connection()
        cur = C_DBAPI.cursor()
        try:
            cur.execute("select * from %s" % (table,)) #XXX SQL INJECTION HERE

        # 3) spool out the current state
        # ---------------------------------------------------
            keys = [col.name for col in cur.description] #low level SQLAlchemy (psycopg2, in this case)
            #keys = cur.keys() #SQLAlchemy
            for row in cur:
                # if we've been told to shutdown
                # fail-fast
                if select.select([shutdown_listener], [], [],0)[0]: # the ",0" is key here! it means "do polling" instead of "do blocking"
                    break

                # else
                # spool the next row
                row = dict(zip(keys, row))  #coerce the SQLAlchemy row format to a dictionary
                #convert row to our made up delta format ("HDRJ")
                delta = {"+": row} #pretend that the existing rows are actually inserts
                delta = json.dumps(delta) #and then to JSON
                yield delta
            # do I need to explicitly close the cursor?
        finally:
            cur.close()
            C_DBAPI.close()

        # 4) spin, spooling out the change stream
        # ---------------------------------------------------
        for delta in changes:
            # we assume that the source (watch_table()) has already jsonified things for us; THIS MIGHT BE A MISTAKE
            yield delta
            # NOTREACHED (unless something crashes, the changes feed should be infinite, and a crash would crash before this line anyway)




def main():
    postgres, table = sys.argv[1:]
    
    assert postgres.startswith("postgresql://"), "Must be a valid sqlalchemy-to-postgres connection string" #XXX there are connection strings which spec the driver to use which are valid too; this assertion is too strong
    #DB_SOCKET = "data" #path to folder containing the postgres socket
    #DB_SOCKET = os.path.abspath(DB_SOCKET) #postgres can't handle relative paths
    #DB_CONN_STRING = "postgresql:///postgres?host=%s" % (DB_SOCKET,)
    global E
    E = sqlalchemy.create_engine(postgres) #TODO: deglobalize

    global shutdown_socket, shutdown_listener
    shutdown_socket, shutdown_listener = socket.socketpair() #when this socket closes all spools (all two of them, but it could be generalized) should shut down immediately
    
    for SIG in [signal.SIGTERM, signal.SIGQUIT]: #XXX do I need to set SIGINT here, or does python's default of raising KeyboardInterrupt serve well enough?
        signal.signal(SIG, shutdown_on_signal)
    
    # start up the thread to monitor stdin for EOF (i.e. when a client disconnects)
    # we do not need to hold onto this thread because it is a daemon and has no clean up to do
    # so we can start it immediately
    threading.Thread(target=alivethread, daemon=True).start()
    
    for delta in replicate(table):
        #print(delta, flush=True) #py3
        print (delta); sys.stdout.flush() #py2/3

    

if __name__ == '__main__':
    main()