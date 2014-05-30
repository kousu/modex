TODO
====

We have a TODO list on a 'real' task tracker at Asana. These should be merged there eventually.

Security
---------

Sandboxing


* How not to do it: https://lwn.net/Articles/574215/
* relevant debate: https://github.com/ctn-waterloo/nengo_gui/issues/25#issuecomment-43095242

http://www.netlab.tkk.fi/opetus/s38153/k2003/Lectures/g41scanners_hackers.ppt

Streaming
---------

see a proposal for how Nengo is doing it: https://github.com/ctn-waterloo/nengo_gui/pull/1/files#diff-5bc02cefb3ea9e27f1a6776eabd1935dR144

CouchDB's [replication filtering](http://couchdb.readthedocs.org/en/latest/replication/protocol.html#filter-replication) ([example](http://guide.couchdb.org/draft/notifications.html#filters)) in `feed=continuous` mode)

http://www.breezejs.com/ which is SQL-in-javascript plus support for databinds, which *might* be what we're looking for. Pro: comes with backends. Con: backends are all microsoftey, except for the MongoDB one.


Orthogonal Tasks
-----------------

_"A Yak Shaved is a Yak Saved"_

These are things that would save us time and headache if we had them,
and that can be worked on indepedently of the critical path.

- [ ] PyGDAL: expand src/models/eutopia/pygdal.py into a full library
    - [ ] all SWIG classes wrapped nicely
    - [ ] unit tests
    - [ ] up on github and *maintained*
    - Lit review:
        - [ ] [PySal](http://pysal.readthedocs.org) -- is this relevant should it be rolled in? supported somehow?
- [ ] "jSQL": some sort of javascript frontend + server backend which allows efficient *and secure* querying (but not updating!) of databases.
      The watchword here is "dynamic": whatever we settle on can't just download a dataset then operate on it, it needs to be able to get new data as new data comes in.
      Ideally, we'd have a nice dataflow library handle this, so that you declare once
       and only once what manipulations (eg transformations like log-log scales, aggregations like means, standard deviations, total morphs like kernel density estimates..) that you are interested in visualizing and on update everything is recomputed (efficiently, even, in the case of simple things like means). this is a pipe-dream at this point.
      And even more ideally, the client-side transformations would become queries which
          limit the results of or (partially?) be performed by the server.
    - [ ] Lit review: read up on how the pros deal with large datasets.
        - [ ] Miso's [Dataset](http://misoproject.com/dataset/) (tables; client side **only**, which is a weakness and a strength)
        - [ ] Square's [Cube](http://square.github.io/cube/) (events)
        - [ ] Square's [Crossfilter](http://square.github.io/crossfilter/) (tables; client side and, somehow, high performance)
        - [ ] [Whisper](http://graphite.readthedocs.org/en/latest/whisper.html)
        - [ ] LINQ (Microsoft: SQL and Objects)
        - [ ] Dublin Core
        - [ ] OWL (objects!)
        - [ ] grano (networks!)
        - [ ] n3/SparQL (objects!)
        - [ ] SQL (tables + relations)
        - [ ] Haskell and Python's list comprehensions (objects!)
        - [ ]  _____????____ (plain, denormalized, one table csv/tsv/xls) approach the task of information storage, organization, and retrieval.
        - [ ] Maps:
            - [ ] http://trac.mapfish.org/trac/mapfish/wiki/MapFishProtocol (GeoJSON) 
            - [ ] See OpenLayers.Protocol.* from [ol2](https://openlayers.org)
            - [ ] WMS/WFS
    - [ ] Implementation
- [ ] wrap modelling framework parameters sweeps into something that Model Runner can use and manipulate
    - [ ] input  (--> shell command line?)
    - [ ] output (--> csv?)
    - for each of:
        - [ ] RepastSimphony
        - [ ] NetLogo
        - [ ] R (this should be relatively easy..)
        - [ ] Python (see: GarlicSIM for inspiration but not dogma)
- [ ] browser visualization lib
    - Mostly this will be collecting things which already exist:
    - [ ] Network
    - [ ] Sankey Diagrams
    - [ ] also requires wrapping each of the above to conform to the DataBind API ([whatever that turns out to be, which we don't have clear yet]) 
- [ ] widgets
    - [ ] build the <slider> tag into a whole HTML5-based date slider
    - [ ] add a <input type="month"> (not date or week, those are too fine; there's no "year" control tho) to give clickable control over time -- [this is only supported in WebKit so far, but by the time we're done the others will have caught up]
    
- [ ] Research and document all the various approaches to watching databases for updates.
      We need this because with the amount of data we plan to handle, polling and computing diffs every cycle will get extremely, extremely expensive.
      Note that, *ideally*, whatever we choose will be portable to Windows too.
    - Unix:
        - Linux's [inotify](http://linux.die.net/man/7/inotify)
        - [FAM](http://oss.sgi.com/projects/fam/), which wraps the above depending on platform
            - Gnome's clone of FAM, [GAMIN](https://people.gnome.org/~veillard/gamin/)
        - OS X's [FSEvents](https://developer.apple.com/library/mac/documentation/Darwin/Conceptual/FSEvents_ProgGuide/Introduction/Introduction.html)
        - *BSD's [kqueue](http://www.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2)'s `EVFILT_VNODE` mode can be used to track changes to files; can it also track changes to folders?
            - [stackoverflow on kqueue gotchas](http://stackoverflow.com/questions/15273061/kqueue-tracking-file-changes-chance-of-losing-events-while-processing-previous#15292041)
    - [Postgres](http://www.postgresql.org/docs/current/static/triggers.html)
    - [MySQL](https://dev.mysql.com/doc/refman/5.7/en/triggers.html)
        - MySQL has replication built in. Maybe similar to CouchDB, we can create a fake MySQL server that listens to the replication stream and pushes it up to the web?
    - Could [CouchDB's Replication Protocol](http://couchdb.readthedocs.org/en/latest/replication/protocol.html) in `feed=continous` be used
    - ...?  

- [ ] Lit review [the dat bug tracker](https://github.com/maxogden/dat/issues) for tips
- [ ] Lit review [the d3 mailing list](FIXME) for tips    

- [ ] Sensible data subsampling [this task will be very, very, very involved]
    - It doesn't seem to be too difficult for javascript (at least on a modernish laptop or better) to store a large amount of data
      (in fact, [Crossfilter can handle 5mb of tables or more](http://square.github.io/crossfilter/)),
       rendering it all is a very different story.
       For maps, mbostock wrote Dynamic Path Simplification, which is now rolled into topojson;
       for line graphs we can do a rolling average or something.
       For a relatively simple relationship (e.g. linear or cubic), a linear subsample (e.g. in python, `s[::1000]` selects every 1000th point) is appropriate--fast, too--but will miss important details applied blindly.
   - The task is: write a library, subroutine, widget, or _something_ which makes culling
     generic datasets to a viewable size reusable and simple.
   - What it means to interact with a piece of scaled UI is complicated!
     If you've averaged points on a line graph, does clicking zoom in to those points or display a popup listing those points?
     If you've path-simplified a map (and just to be fiddly, suppose the simplification happened to blur provincial boundaries),
       does clicking a block show data from both provinces?
       Getting this right means the simplification cannot throw away all of the relationships, but it is unclear what it should keep.
   - Lit review:
       - [ ] Microsoft's [SemanticZoom](http://msdn.microsoft.com/library/windows/apps/hh702601)
       - [ ] Audacity's [waveform display](https://svn.FIXME) switches rendering methods below a certain hardcoded threshold -- but this is strictly only appropriate for waveforms, like audio and maybe seasonal timeseries.
       - [ ] [imMens: Real-time Visual Querying of Big Data](http://vis.stanford.edu/files/2013-imMens-EuroVis.pdf). Section 3 summary:
           - Filtering - only show data that fall within certain data ranges (e.g. filter by a limited time range)
           - Sampling - only show a random subset of the data (e.g. include each data point with a 10% chance)
           - Binned Aggregation - compute aggregated values (e.g. sum or average) over windows of the data space (e.g. count of events per day)


- Load Tests: we have a lot of uncertainty about what the right APIs to use.
  To clear the air, we should task someone to focus on writing reproducible tests which
   systematically determine performance (e.g. runtime, number of items renderable, number of connections available, etc)
   on each of our platforms:
       {IE, Firefox, Opera, Safari, iOS Browser, Android Browser, Chrome, *old* IE} cross 
       {WindowsXP, Windows7, Windows8, Linux, Ubuntu, OS X} 
      Use tools like [Selenium](http://docs.seleniumhq.org/) and [Wati{N,r}](http://watin.org/).
    - Data streams:
       {WebSockets, REST, SOAP, XMLRPC, json-rpc, ...}
        - [ ] How many can be open at once?
        - [ ] 
    - SVG (see http://trac.osgeo.org/openlayers/wiki/Future/OpenLayersWithCanvas for the sort of report we're looking for)
        - [ ] How many {points, lines, paths} before the browser crashes?
        - [ ] How does scrolling the image lag?
    - 
    - [ ] d3 on svg vs d3 on canvas
        - (while you're at it, find solid demos and link them under `scratch/` to show how to do this)
- Make the SQL extraction efficient; python's default is to print 16 bytes of ASCII for each Decimal, even if it is the value "1.0000000000". Also, look into binary serialization (msgpack, binary csv, etc); possibly an extension to jsSQL specifying sigdigs to keep (which, actually, would be easy to do with python's Decimals)

- [ ] Write a SQL hook that exports the database to the filesystem. It might even be possible to do this [via](http://apidoc.apsw.googlecode.com/hg/vtable.html) [python](http://multicorn.org/)
- [ ] Write a NoSQL hook that exports the database to the filesystem.
- [ ] Use inotify and/or FAM to watch the above for deltas and push the deltas up to the web.

- [ ] linkup dataprotocols ppl to the dataarchives ppl
 that one social science archive has a standard. it's crufty, but it exists