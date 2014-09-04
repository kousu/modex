#!/usr/bin/env python3
# view.py
"""
pure-python protoype of the sort of features we need in a (near-)real-time View.
The goal is to support dataflow (aka Functional Reactive aka actor) programming on top of SQL.
It is unfortunate that something like this does not already exist within postgres itself. At least,
I have not been able to find such a thing. Instead, this fronts onto postgres, using some stored procedure
shims which spool out all triggers.

This is in python3
watch.psql is mainly in python2
This awkwardness will be massaged out later.


NB: this was attempt 1; attempt 3, which turned out successful, is the pair of files replicate.py{,sql}.
    this still has a lot of useful logic and structure in it, and should be merged in later.
"""



"""
Bugs:


goal: de-hardcode the query
 --> leave 'where' and 'columns' hardcoded to None (ie all) b/c those are unnecessary right now
 --? 
 
goal: stuff websockify in front of view_server ("viewd"? ) ;; maybe view is a bad name? it's a replication? it's "filtered replication".. or, what does FRP call this?
 --> pick better names... 

goal: think through how to rewrite this in terms of logical replication, and map out the pros/cons of that

goal: solve select() on a pipe is returning even when there is nothing to read, forcing me to use a polling loop
    write some experiments to test select() vs pipes
    
experiment with multicast UDP instead of mkfifo + teed

goal: clean up the internal server debugging prints, because I don't need those...

goal: document how to run / make running less stupid
 -> rename server.sh to db_server.sh and such; and db_server.sh should also kick initdb off if it hasn't run lately
 -> maybe use libpq environment variables (but then things are annoying to clean up..)
 
embed watch.psql into view.py 
 -> split it so that the watching function is loadable, and we (initially we can just always reload it)
 -> make the part that actually initially
  or possibly a better solution is to use DDL triggers so that every table has a watch on it...


bug: DELETE FROM table; doesn't flush its results immediately??


far-hanging-fruit
- catch DROPs





my API looks like what?


on the js side:
//////////// internal structure

function replicate(...) {
  
  w = WebSocket()
  w.onmessage(function(e) {
    queue.push(parse(e.data))
  })
  
  
  return obj;
}


////////////// API


source = replicate("ws://server/replicant/databasename/tablename/", columns, where) ?
 i.e. there's (and possibly 
  ...well, the code that installs watchers is going to have to exist *somewhere*
   either it exists in something that speaks HTTP reading the URLs (flask? flask would be quick for this,
    and additionally, since there needs be only one of these processes per server, could just have an internal "i have installed the watcher on this table" table)

 to be clear:
  there's an HTTP process which manages o
  
  do I need to write
  
"./replicate server?? database?? table [columns] [where]"
 i think i dooo
 so i write that... then what?
 ^ this is the unix way..
^ "replicate" is basically this file; its output
then that... pipes stdout to.. somewhere????? ultimately it will need to have multiple teed
 is it even possible for me to construct native unix pipes between processes within python? I suppose I can do yet another bash wrapper script.
 oh, useful: websockify has a mode where it exec()s a process; ie it can websockify *either* an existing regular socket, or a program. maybe I don't need socat afterall.
  --> oh, but something about my build has this broken; which is fine, because the unix way would be that websockify does exactly one thing, which is to speak (and if you need something fancier, that's what (socat or nc) is for)
  a problem with websockify is that it ignores the path; in fact, the only thing it does with the HTTP path is a logging line:
```
              if self.path != '/':
                self.log_message("%s: Path: '%s'", client_addr, self.path)
```
 is it possible for me to do file descriptor handoff? I think so... fork+exec will hand off stdin and stdout to a new process; also, linux has an IPC hack called [???] which allows "sending" FDs to other processes if you need more than that.
 i want an HTTP server spinning, and *if* we decide that the incoming connection is for a replicant websocket, then I want to fork off to websockify speaking to
  ...wait; isn't websockify just basically
   ugh, problem is that unix sockets are *not* just pipes. because pipes read and write streams, and sockets were designed around packets; so you can't do fd; at least, you can't do it and have it work right. you need socat sitting in the middle. 
   
   server listen()s for HTTP
    and spins, handling HTTP requests
   and um
   if you are addressed to a replicant session (i.e. path is appropriate and), it [does stuff] then spawns a replicant.py
 (( the difference between replicant.py and the _changes stream is that replicant prefixes the *current* table state before sending; and, once we're past demo day and get to it, it will do filtering and delta compression and maybe even row-by-row compression (e.g. a set of rows all of which change name from 'a' to 'b' only need to be instructed to do such once)))
 ((later on, we'll do something clever with replicant.py such that, while each connected session receives a unique prefix of rows, the changes stream is only computed once per combination of parameters))
 
 i want the kernel to do this. i *do not want* to write "loop: write(output, read(input))" because that pulls the data through a buffer in userland; if i must write that, then i want it to be contained in socat, which has been designed to do this very well.
 
 another irritant of websockify is that it insists on 'binary' or 'base64' encoding; but I've used websockets just sending strings back and forth and had it behave itself fine.
 
or do I have to do
source = replicate("ws://server/replicant/", "databasename", "tablename", columns, where)
corresponding to
w.send('{"db": "databasename", "table": "tablename", "columns": "columns", "where": where}') //NB: 'where' is not spec'd yet; in fact, I am going to leave it unspecced because spec'ing it means redefining SQL or doing some sort of awkward exposure of SQL to the front end

^ this is easier for me right now, so I'm going to do this. 
 the other way is better because knowing that a table watcher has been installed isn't as finicky
 
 
 
 
Sketch of Components:
* watchers: the shim of code which gets extracts database updates to a FIFO;
   currently implemented as a python stored procedure
   When pg9.4 is released, could be implemented by Logical Replication, though that requires editing a config file instead of just loading some .sql
* replicants: the code which accepts a client session and feeds out changes to a particular slice
  Currently implemented in python, and our slice is only (table,), though (database, table, columns, where)
* websocket bridge: something which connects
  implementation: some combination of websockify + socat 
* database: postgres 9.x, in our case
* simulation(s): a concurrent process which feeds the database; writes (inserts, updates, and deletes (though, for scientific purposes, we should almost never be using updates or deletes; any time we need to go over old territory, instead we should just generate a new simulation ID and append that to all rows)) that the simulation causes should be reflected immediately in the database -> watcher -> replicant -> client chain

* clients: clients watch the database state by opening a websocket to the bridge
    through an out of band means (e.g. another websocket, likely), clients can control the simulation and hence, indirectly, the values they read from the database. However, the simulation+database 

"""

import sys
import json #for IPC; this will get replaced with something more tuned for high-volume later
#import sqlalchemy

import select

from itertools import count

# TODO: support replicators

def debug(*args, **kwargs):
    if "file" not in kwargs: kwargs["file"] = sys.stderr
    print(*args, **kwargs)

class ChangeEvent(object): #abstract base class
    pass
    
class Insert(ChangeEvent):
    def __init__(self, row):
        self.row = row
    def __str__(self):
        return json.dumps({"+": self.row})
        
class Delete(ChangeEvent):
    def __init__(self, row):
        self.row = row
    def __str__(self):
        return json.dumps({"-": self.row})
        
class Update(ChangeEvent):
    def __init__(self, old, new):
        self.old = old
        self.new = new
    def __str__(self):
        return json.dumps({"-": self.old, "+": self.new})

class ITableChanges(object):
    def next(self):
        raise NotImplementedError("This is an interface definition only. Subclass and override.")


class PipeChanges(ITableChanges):
    "parses change events as serialized by watch.psql"
    """
     the serialization format is:
      a json object that looks like:
        {"+": row} for inserts, or
        {"-": row} for deletes, or
        {"-": row, "+", row} for updates
      where a row is a jsonification of a SQL row, ie.
     This is not the most efficient protocol we could use, but it's dead simple to program against.
    """
    def __init__(self, stream):
        "stream should be an open file object in +r mode"
        self._source = stream
    
    def __next__(self):
        
        evt = ""
        while not evt: #hack around empty lines being a problem?!
            a,b,c = select.select([self._source], [], []) #blocks until self._source is ready
            debug("select result:", a, b, c)
            evt = self._source.readline() #blocks here; we could use select() but since we only wait on one 
        # XXX bug: readline() doesn't block on EOF (which python passes us as "")
        # patch: use `tail -f` instead of `cat`
        # XXX flakiness: running multiple readers can make the other reader steal some of the bytes and crash the JSON deserialization
        # XXX flakiness(?): postgres apparently doesn't call *any* triggers until the session (ie the database connection) that created them is closed(??)
            evt = evt.strip()
            if not evt:
                import time; time.sleep(0.1) #durp; i would rather select(), but polling is alright if I must.
            else:       
                debug("_next() read '%s'" % (evt,)) #DEBUG
                pass
        
        evt = json.loads(evt)   #TODO: handle exceptions here
        if "+" in evt and "-" in evt:
            return Update(evt["-"], evt["+"])
        elif "+" in evt:
            return Insert(evt["+"])
        elif "-" in evt:
            return Delete(evt["-"])
        else:
            warn("Malformed change event: `%s`" % evt)

class View(object):
    """
    A View is a slice of a database. For example, "all farms or farmers who existed before 1944".
    This implementation is special because it is dynamic. It takes a stream of changes,
    filters them, and then provides a smaller stream of changes which consumers
     up to date in real time with the state of the view.
    In other words, this implements filtered replication ([like CouchDB](http://docs.couchdb.org/en/latest/replication/intro.html#controlling-which-documents-to-replicate)),
    but for SQL.
    Standard views in SQL regenerate every time they are queried, and similarly Postgres's MATERIALIZED VIEWS (basically a cached view) can only be refreshed once they are stale with a full rescan.
     All it needs is some water and a source of row-level change events
     (e.g. as prototyped in watch.psql).
     

    
    LIMITING ASSUMPTIONS:
    * the source of change events feeds full rows
    * the only sorts of changes are inserts, updates, deletes
    * the schema never changes
    * ..but the schema is also never explicitly stated (and the end target is d3-style array-of-rows JSON)
    * you can only slice tables, not databases, with this (ie there are no JOINs or other fancy operators)
    
    TODO:
    * Optimizations
      * [ ] If that table has a primary key, deletes can be compressed to just send that
      * [ ] Updates can be compressed be removing the common terms
      * Ideally, this feature would be fast, in C, inside of Postgres, triggering on updates to parents of MATERIALIZED VIEWS, and would properly handle the full range of SQL (JOINs, SET a = a + 1, etc).
    """
    
    def __init__(self, changes, columns=None, where=None):
        "changes is a stream of changes from a single table that you're watching" #XXX this def'n is awkward; I don't know what is ideal yet.
        "columns is a list of; defaults to all columns"
        "if specified, where should be a predicate taking a row and telling whether it is in the view or not"
        if where is None:
            where = lambda *args: True
        
        self._source = changes
        #if columns is None:
        #    columns = parent.get_all_columns_XXX_THIS_CALL_DOESNT_EXIST_and_maybe_it_doesnt_need_to()
        self._columns = columns
        self._where = where
    
    
    def push(self, evt):
        self._queue.push(evt) #TODO: locking, etc
    
    def _publish(self, evt):
        if evt is None: return #silently make empty events into no-ops 
        print(evt) #publish the event; for us, this elegantly just means printing; THIS SHOULD BE THE ONLY PRINT TO STDOUT IN THE PROGRAM.
        sys.stdout.flush()     #XXX hammer around buffering issues which make testing confusing
    
    
    def _cull(self, row):
        "reduce a row (given as a dictionary) to only the columns (ie dict keys) that are in self.columns)"
        if self._columns: #if *not* self._columns, pass through all columns
            row = {k: row[k] for k in self._columns} #XXX this might be a speed bottleneck
        return row
        
    def _process(self):
        evt = next(self._source)
        #evt = self._queue.pop(0) #TODO: locking/blocking (python's got a native library for this, right?)
        # the code below must happen at some point... maybe not in a subroutine like this. then again, maybe so.
        # evt is a change event; for our purposes, this means it has a .type field which is either ">" (update), "+" (insert) or "-" (delete) followed by
        
        if not (isinstance(evt, ChangeEvent) and (not type(evt) is ChangeEvent)):
            raise TypeError
        
        # map source events to events on the table that the view is pretending to be
        # that is: censor events on rows which the view is not interested in,
        #          cull rows down to , and
        #          potentially convert updates to inserts or deletes
        if isinstance(evt, Insert) or isinstance(evt, Delete): #TODO: polymorphism?
            if self._where(evt.row): #pass inserts and deletes through unscathed, if they pass the filter
                evt = evt
            else:
                evt = None #do not publish; this row is not in the view
        elif isinstance(evt, Update):
            # updates are subtler
            # every update could be viewed as a deletion and insertion, but that's inefficient and has atomicity issues.
            # instead, follow this logic:
            # an update where both sides, old and new, are in the view, is an update to the view
            # an update where only one side is in the view is, as far as the view's receipients can tell, is an update
            # 
            _oIn = self._where(evt.old) #precache, to avoid hitting the predicate more than necessary
            _nIn = self._where(evt.new)
            if _oIn and _nIn: #both old and new states of the view see this row; pass it through as an update unscathed
                evt = evt
            elif _nIn:
                evt = Insert(evt.new)
            elif _oIn:
                evt = Delete(evt.old)
            else:
                evt = None #do not publish; this row is not in the view
        
        # filter down the rows 
        if isinstance(evt, Insert) or isinstance(evt, Delete):  #TODO: polymorphism? 'isinstance' is usually a sign that you should be using polymorphism
            evt = type(evt)(self._cull(evt.row))
        elif isinstance(evt, Update):
            evt = type(evt)(self._cull(evt.old), self._cull(evt.new))
        
        # XXX ^ the above flow can probably be simplified by rethinking the ChangeEvent hierarchy
        debug("FINALLY, evt is ", evt) #DEBUG
        self._publish(evt)



if __name__ == '__main__':
    
    #def view([columns], table, where=None)
    #  - zeroth step is to parse the initial lines from the client, which contain the query: (columns, table, where)
    #  - first step is it tells the query to the DB ("select [columns] from table where ...")
    #  - second it arranges to receive the feed (note: feed == stream == queue; it is ordered and waits for us to pull from it)) of changes for the given table ((this might involve setting up triggers? it also might involve speaking to 'teed')); I *think* the order is important here: we want to receive changes beginning exactly at the timestamp after the one the query is slicing. All methods besides the one I want will lead to a race condition here.
    #  - third it spools out the result of the query to the client, as a stream of + changes; because of [MVCC](https://wiki.postgresql.org/wiki/MVCC), this spools out
    #  - finally, it transparently switches over to spinning through the feed of changes, doing the filtering as it goes
    
    # STEP ZERO: parse request
    req = sys.stdin.readline()  #admittedly, the unix way
    req = json.loads(req)
    
    # hardcoded columns
    #columns = ["name", "rating"] #nb. None ~= all
    columns = None
    
    # hardcoded table
    #table = "films"
    table = req["table"]
    
    # hardcoded where predicate
    #def w(row):
    #    debug("where() clause received row", row)
    #    return row["rating"] >= 3
    w = None
    
    # ** the above three should be replaced by some code which parses the initial request    
    
    # STEP ONE: query current state
    # ** we skip doing this for now; it is as if the table always starts empty, which is fine for testing
    
    # STEP TWO:
    # ** we hardcode directly reading from the changes feed
    with open("data/_changes_%s" % (table,)) as feed:
        # STEP THREE: spool the initial DB state (ie its state at query-time) out
        # ** skipped, since step two is skipped
        
        # STEP FOUR: spool changes out
        #
        v = View(PipeChanges(feed), where=w)
        for i in count():
            debug("-"*80)
            debug("step ", i)
            v._process()
        
