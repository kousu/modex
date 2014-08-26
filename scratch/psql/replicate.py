#!/usr/bin/env python
# usage: replicate table --> stdout posts a line-oriented stream of HRDJ
# third-time's-a-charm edition
# depends: sqlalchemy

# depends: watch.pysql and replicate.pysql are loaded into postgres

"""
TODO:
 irritant: select() blocks ctrl-c, because it's an I/O wait (quick fix: give a long timeout on select and spin it in a polling loop)

clean up the sql injections


 


"""

import sqlalchemy


import os, tempfile, socket, select
import json
import os


DB_SOCKET = "data" #path to folder containing the postgres socket
DB_SOCKET = os.path.abspath(DB_SOCKET) #postgres can't handle relative paths
DB_CONN_STRING = "postgresql:///postgres?host=%s" % (DB_SOCKET,)
E = sqlalchemy.create_engine(DB_CONN_STRING)
C = E.connect()

#import IPython; IPython.embed()

# We are allowed to have multiple ResultProxies open during a single connection.
# 


  # this is code that should be library code
  # but installing it such that postgres can read it
  # and without stomping on other things too badly is hard
  # so for now it is just loaded here over and over again
class Changes:
    MTU = 2048 #maximum bytes to read per message
    
    def __init__(self, table): #TODO: support where clauses
      self._table = table
      self._stream_id = None
      
    def __enter__(self):
      # set up our listening socket
      self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
      self._sock.bind(tempfile.mktemp())
      print("listening for changes to %s on %s" % (self._table, self._sock.getsockname()))
      
      # register ourselves with the source
      # XXX SQL injection here
      with C.begin():  #very odd: something about the interaction of SQLAlchemy and stored procedures means that any writes the stored procedure does stay uncommitted forever. However, explicit commit commands (which the transaction created by C.begin() causes) 
        r = C.execute("select * from my_pg_replicate_register('%s', '%s')" % (self._table, self._sock.getsockname()))
        r = r.scalar()
        print("register() result is: ", r)
        
        self._stream_id = r
      
      return self #oops!
    
    def __exit__(self, *args):
      # unregister ourselves
      # XXX SQL injection here
      with C.begin():
        r = C.execute("select * from my_pg_replicate_unregister('%s')" % (self._stream_id))
        r = r.scalar()
        print("unregister() result is: ", r)
      
      # shutdown the socket
      os.unlink(self._sock.getsockname()) # necessary because we're using unix domain sockets
      self._sock.close()
    
    def __iter__(self):
      return self
    
    def __next__(self):
      fread, fwrite, ferr = select.select([self._sock], [], [self._sock])  #block until some data is available
      if ferr:
        pass #XXX
      else:
        pkt = self._sock.recv(Changes.MTU)
        pkt = pkt.decode('utf-8')
        return pkt
    
    next = __next__ #py2 compatibility
  

def replicate(_table):
  # 1) get a cursor on the current query
  #plan = plpy.prepare("select * from $1", ["text"]) # use a planner object to safeguard against SQL injection #<--- ugh, but postgres disagrees with this; I guess it doesn't want the table name to be dynamic..
  #print("the plan is", plan)
  #cur = plpy.cursor(plan, [_table]);
  # stream_results is turned on for this query so that this line takes as little time as possible
  cur = C.execute("select * from %s" % (_table,))
  # .execution_options(stream_results=True)
  
  
  
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
  with Changes(_table) as changes: #<-- use with to get the benefits of RAII, since Changes has a listening endpoint to worry about cleaning up
    
  # 3) spool out the current state
  # ---------------------------------------------------
    for row in cur:
      row = dict(zip(cur.keys(), row))  #coerce the SQLAlchemy row format to a dictionary
      delta = {"+": row} #convert row to our made up delta format; the existing rows can all be considered inserts
      delta = json.dumps(delta) #and then to JSON
      yield delta
    # do I need to explicitly close the cursor?
  
    C.close() # we don't need to talk directly to the DB anymore at this point; it will feed us changes out-of-band via our socket
    
  # 4) spin, spooling out the change stream
  # ---------------------------------------------------
    for delta in changes:
      # we assume that the source (watch_table()) has already jsonified things for us; THIS MIGHT BE A MISTAKE
      yield delta
    # NOTREACHED (unless something crashes, the changes feed should be infinite, and a crash would crash before this line anyway)
    
    
if __name__ == '__main__':
    import sys
    table = sys.argv[1]
    
    for delta in replicate(table):
        print(delta)
    
    # NOTREACHED
    