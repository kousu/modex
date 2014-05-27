sqldiff
=======

An algorithm for computing deltas on tables.

The goal is to be able to turn two snapshots
of a database into a stream of three sorts of events:

* `add({column1: value1, column2: value2, ..., columnN: valueN})` - add a new row 
* `update(rowid, {column2: updated_value2, column3: updated_value3, column5: updated_value5, }...)` - this only sends the updated columns, not the full set
* `delete(rowid)

Instead of updates, we can do a delete and an add at the same time.

Methods Considered
------------------

1. naive: for each line in the first set, find if it's in the second and if not add to deleted list
 for each line in the second set, find if its in the first and if not add to the first 


2. Compute the set differences of the tables:
   - the leftovers of the original (left ) table are the deleted lines
   - the leftovers of the      new (right) table are the added lines
   
   To do this efficiently:
    - sort the inputs;
    - travel through them side by side
    - search for identical pairs of lines and mark them as removed
      (equivalently, add the non-removed lines to the output sets as you go)
    - (due to the sort, you only need to check each row once)

2.i 
  compute set differences but find
   [...?]
  
(2,1) could be implemented in SQL stored procedures, logging results to an "events" table; every time you call the procedure it erases the old table and writes a new one

3. Find a way read the SQL log directly 
  (the equivalent with a journalling filesystem like ext3/ntfs/btrfs would be to parse the log; but not all SQLs necessarily keep such a log
  -
  - Implement the MySQL/Postgres/CouchDB/etc replication protocol in javascript and plug it into a WebSocket

4. construct our own journal
     - use SQL triggers (MySQL, MSSQL, Postgres and Sqlite all have some extension method which you can get to do actions on CREATE, INSERT, UPDATE and DELETEs) and pass the events via IPC/filesystem queue/socket/HTTP
     - rig up a way to export to the filesystem, and then use FAM/inotify/kqueue
         - sqlfuse
         - the sqlite vfs hook
         
5. add timestamp column with automatic updates
  then to get the changes, you do SELECT * from table where ts > last_time_loaded
  ^ but this doesn't handle DELETEs

Large Architecture
------------------

The most flexible and reliable way to plug this into a larger system is message-passing.

What we really do not want to be doing is rerunning diff() every millisecond for every user,
but that is what we might end up doing if we aren't careful, because generally every client will be at a slightly different place in the history
If we instead have a central diffing agent which runs say, every 5 seconds
and sends out a stream of change events to a queue
and those get pushed up to a bus
upon which clients (well, the websocket endpoint that is proxying for the client) registers listeners.
 and pull down the events into their own queues
 for playing out to clients
 (and that way, if a client lags out its queue of deltas will not get misordered; they'll just wait and then batch)

The central diffing agent will slow linearly (or worse, if badly programmed) as the database grows;
it might get to a point where it takes longer to compute the diff than the time allotted.
This seems silly, considering that in that case, most rows will remain unchanged.
(dat must have this solved; how did they do it?)

If we instead had a way (e.g. options 3, 4) to access the change events
 directly we could replace the central diffing agent, and keep all the 
 flexibility of the event-passing architecture. 

TODO
----


* [ ] Extend to support delta compression, which is when deltas which cancel each other out in the stream (?? is )
    * ^hmmmmmmmmmm. I *think* that any single run of this algorithm should already cover that case cleanly. delta compression only comes in when you have a series of deltas in a row.
    oh that's interesting then. so we have a time-space tradeoff; or even space in two ways: "physical" space and "delta" space. Delta compression only kicks in if the recipient has lagged out and fallen behind. That is appropriate for dropbox, where the point is syncing with long-term disconnects in between. It's not so important for us, where we want a real-time stream. 
* [ ] Look into using [LP solvers in python](https://wiki.python.org/moin/NumericAndScientific/Libraries#Mixed_Integer_and_Linear_Programming)

References
-----------

* DataProtocols' [Tabular Diff Format](http://dataprotocols.org/tabular-diff-format/)
* [DiffKit](http://www.diffkit.org/)
    * [daff](http://paulfitz.github.io/daff/) <-- js implementation
    
    
    
Thanks
-------

* Jenny Wong <jy2wong@uwaterloo.ca> and Marc Burns <m4burns@uwaterloo.ca> for the pointer to the [Hungarian Algorithm](https://en.wikipedia.org/wiki/Hungarian_algorithm) and [Bipartite Matchings via Linear Programming](http://www.imsc.res.in/~meena/matching/lecture5.pdf).
    