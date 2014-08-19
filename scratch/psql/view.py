#!/usr/bin/env python3
# view.py
"""
pure-python protoype of the sort of features we need in a (near-)real-time View.
A standard

This is in python3
watch.psql is mainly in python2
This awkwardness will be massaged out later.
"""

import sys
import json #for IPC; this will get replaced with something more tuned for high-volume later
#import sqlalchemy

import select

from itertools import count

# TODO: support replicators

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
            #print("select result:", a, b, c)
            evt = self._source.readline() #blocks here; we could use select() but since we only wait on one 
        # XXX bug: readline() doesn't block on EOF (which python passes us as "")
        # patch: use `tail -f` instead of `cat`
        # XXX flakiness: running multiple readers can make the other reader steal some of the bytes and crash the JSON deserialization
        # XXX flakiness(?): postgres apparently doesn't call *any* triggers until the session (ie the database connection) that created them is closed(??)
            evt = evt.strip()
            if not evt:
                import time; time.sleep(0.1) #durp; i would rather select(), but polling is alright if I must.
            else:       
                print("_next() read '%s'" % (evt,))
        
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
    
    def __init__(self, changes, columns=None, where=lambda *args: True):
        #"parent is a table to watch"
        "changes is a stream of changes from a single table that you're watching" #XXX this def'n is awkward; I don't know what is ideal yet.
        "columns is a list of; defaults to all columns"
        "if specified, where should be a predicate taking a row and telling whether it is in the view or not"
        self._source = changes
        #if columns is None:
        #    columns = parent.get_all_columns_XXX_THIS_CALL_DOESNT_EXIST_and_maybe_it_doesnt_need_to()
        self._columns = columns
        self._where = where
    
    
    def push(self, evt):
        self._queue.push(evt) #TODO: locking, etc
    
    def _publish(self, evt):
        if evt is None: return #silently make empty events into no-ops 
        print("publishing", evt)
        pass #???
    
    
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
        
        self._publish(evt)



if __name__ == '__main__':
    #view() (pretend view.py is a function which takes some args and then spits a stream back; that's actually jsut waht it does niggah) 
    #def view([columns], table, where=None)
    #  - zeroth step is to parse the initial lines from the client, which contain the query: (columns, table, where)
    #  - first step is it tells the query to the DB ("select [columns] from table where ...")
    #  - second it arranges to receive the feed (note: feed == stream == queue; it is ordered and waits for us to pull from it)) of changes for the given table ((this might involve setting up triggers? it also might involve speaking to 'teed')); I *think* the order is important here: we want to receive changes beginning after the query happened. Unless we specifically create (perhaps in the same query?).
    #  - third it spools out the result of the query to the client, as a stream of + changes; because of [MVCC](https://wiki.postgresql.org/wiki/MVCC), this spools out
    #  - finally, it transparently switches over to spinning through the feed of changes, doing the filtering as it goes
    
    # STEP ZERO: parse request
    # hardcoded columns
    columns = ["name", "rating"] #nb. None ~= all
    
    # hardcoded table
    table = "films"
    
    # hardcoded where predicate
    def w(row):
        print("where() clause received row", row)
        return row["rating"] >= 3
    
    # ** the above three should be replaced by some code which parses the initial request    
    
    # STEP ONE
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
            print("-"*80)
            print("step ", i)
            v._process()
            print("-"*80)