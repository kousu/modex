# view.py
"""
pure-python protoype of the sort of
"""



class ChangeEvent(object):
    pass
    
class Insert(ChangeEvent):
    def __init__(self, row):
        self.row = row
        
class Delete(ChangeEvent):
    def __init__(self, row):
        self.row = row
        
class Update(ChangeEvent):
    def __init__(self, old, new):
        self.old = old
        self.new = new

class View(object):
    """
    A View is a slice of a database. For example, "all farms or farmers who existed before 1944".
    This implementation is special because it keeps consumers up to date in real time with the state of the view.
     given a source of row-level change events (e.g. as prototyped in watch.psql).
     
    This implements filtered replication ([like CouchDB](http://docs.couchdb.org/en/latest/replication/intro.html#controlling-which-documents-to-replicate)),
    for SQL. Standard views in SQL regenerate every time they are queried, and similarly Postgres's MATERIALIZED VIEWS (basically a cached view) can only be refreshed once they are stale with a full rescan.
    Ideally, this feature would be fast, in C, inside of Postgres, triggering on updates to parents of MATERIALIZED VIEWS, and would properly handle the full range of SQL.
    
    LIMITING ASSUMPTIONS:
    * the source of change events feeds full rows
    * the only sorts of changes are inserts, updates, deletes
    * the schema never changes
    * you can only slice tables, not databases, with this (ie there are no JOINs or other fancy operators)
    
    TODO:
    * Optimizations
      * [ ] If that table has a primary key, deletes can be compressed to just send that
      * [ ] Updates can be compressed be removing the common terms
    """
    
    def __init__(self, parent, columns=None, where=lambda *args: True):
        "parent is a table to watch"
        "columns is a list of; defaults to all columns"
        "if specified, where should be a predicate taking a row and telling whether it is in the view or not"
        self._parent = parent
        if columns is None:
            columns = parent.get_all_columns_XXX_THIS_CALL_DOESNT_EXIST()
        self._columns = columns
        self._where = where
    
    
    def push(self, evt):
        self._queue.push(evt) #TODO: locking, etc
    
    def _publish(self, evt):
        print("publishing", evt)
        pass #???
        
    def _process(self):
        evt = self._queue.pop(0) #TODO: locking/blocking (python's got a native library for this, right?)
        # the code below must happen at some point... maybe not in a subroutine like this. then again, maybe so.
        # evt is a change event; for our purposes, this means it has a .type field which is either ">" (update), "+" (insert) or "-" (delete) followed by
        
        assert (isinstance(evt, ChangeEvent)) and (not type(evt) is ChangeEvent)
        
        if isinstance(evt, Insert) or isinstance(evt, Delete): #TODO: polymorphism?
            if self._where(evt.row): #pass inserts and deletes through unscathed, if they pass the filter
                self._publish(evt)
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
                self._publish(evt)
            elif _oIn:
                self._publish(Delete(evt.old))
            elif _nIn:
                self._publish(Insert(evt.new))
            else:
                pass #do not publish; this row is not in the view