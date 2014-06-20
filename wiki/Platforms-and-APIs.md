Here we list all the APIs and platforms we have considered. It is a serious time sink to research the suitability of a platform, so with the reasons we have kept or rejected, or put  each on hold.


# Frontend

HTML5 is extremely powerful. It has a lot of new widgets (under form elements: sliders, numbers, dates, file uploaders, also <progress> and <meter> which lists). We can probably build most of our widgets direct in HTML.

* LessCSS
* craftyjs?
* [TogetherJS](https://togetherjs.com/) by Mozilla for adding commenting throughout a site
* [Modernizr](http://modernizr.com/) to let us write one codebase against HTML5

* Promises: [overview of some of the better implementations](http://www.promisejs.org/implementations/) / [a different site??](http://promisesaplus.com/implementations)
  * ayepromise (**<-- current**)
  * when.js
  * promise.js
  * ....

**Layout Templates**
* [Boilerplate](http://html5boilerplate.com/)
* [Bootstrap](http://getbootstrap.com/)

## Visualizationw

* d3js
* Square's [cubism.js](http://square.github.io/cubism/) for visualizing live time series data
* [graphite](http://graphite.wikidot.com/)
* Native HTML5 (e.g. see [our html5 widget demo](../../scratch/html5/widgets))
* Spreadsheet widgets (more [@](http://plugins.jquery.com/tag/spreadsheet/))
    * [Handsontable](http://handsontable.com/)
* [Vispy](http://vispy.org/) (_not a frontend possibility, but targetted at real-time big-data and interactivity, so good to keep in mind_)

### Encapsulated Visualization Libraries

* [Mike Bostock's Reusable d3 chart spec](http://bost.ocks.org/mike/chart/)
  * and the [official results](https://github.com/d3/d3-plugins), so far 
  * 
* [dimplejs](http://dimplejs.org/) - _this looks pretty underpowered_
* [Vega](https://github.com/trifacta/vega)
  * [Vincent](https://github.com/wrobstory/vincent) - _A Python to Vega translator_
* [NVD3](http://nvd3.org/)
* [Miso](http://misoproject.com/)   - _this one looks super promising_
* [DexCharts](https://github.com/PatMartin/DexCharts)
* [Flot](http://www.flotcharts.org/)
* [Chart.JS](http://www.chartjs.org/)
* Scrap reusable d3 examples:
  * http://jsfiddle.net/johnwun/8hSGP/
  * [textrotate()](http://bl.ocks.org/ezyang/4236639)
  * [pie-chart](https://github.com/gajus/pie-chart)
  * [interactive histograms](https://github.com/gajus/interdependent-interactive-histograms)
  * ["slopegraph"](http://bl.ocks.org/biovisualize/4348024) (actually a very basic network visualization)
  * [demo of building a reusable component from a nonreusable one](http://bl.ocks.org/milroc/5519642)
  * [messy errorbar scatterplot](http://bl.ocks.org/chrisbrich/5044999)
  * ["hello world" in reusable d3](http://bl.ocks.org/cpbotha/5073718)
  * [simple bar chart](http://jsfiddle.net/johnwun/8hSGP/)


### Maps

* OpenLayers: [2](http://openlayers.org) and [3](http://ol3js.org)
  * we should see if we can divert some part of our budget to them: http://www.indiegogo.com/ol3
* http://tilestache.org/
* http://polymaps.org/
* [ModestMaps](http://modestmaps.com/examples/)
(_these all seem like they overlap a bit; what's the deal?_)


## Networking

* [WebSockets](http://www.websocket.org/quantum.html)
  * [Websockify](https://github.com/kanaka/websockify) -- a short WS<->TCP proxy which treats websockets like they were meant to be treated
  * Autobahn
    * Tutorials do not work at this time. Re-evaluating our use of Autobahn.
  * [SockJS-twisted](https://github.com/DesertBus/sockjs-twisted/); see also [SockJS-client](https://github.com/sockjs/sockjs-client) for drop-in websocket support for older browsers
  * [twisted.web.websockets](https://twistedmatrix.com/trac/ticket/4173) is in [the works](http://twistedmatrix.com/trac/attachment/ticket/4173/4173-5.patch), but not ready yet
  * [ws4py](https://github.com/Lawouach/WebSocket-for-Python)
  * [cherrypy](http://www.cherrypy.org/)
  * [txWS](https://github.com/MostAwesomeDude/txWS)
  * [socket.io](http://socket.io/)
      * [python API](http://gevent-socketio.readthedocs.org/)
      * [other python API](https://github.com/MrJoes/tornadio2)
  * [JSON-RPC](https://en.wikipedia.org/wiki/JSON-RPC) is dead simple and worth investigatign

* [EventSource](http://stackoverflow.com/questions/8499142/html5-server-side-event-eventsource-vs-wrapped-websocket)
* jsonp

## Serialization

* json
* [https://github.com/edn-format/edn](edn)
* msgpack (json in
* http://nytimes.github.io/tamper/ - _achieves superior compression via categorical data_
* [protobufs](https://developers.google.com/protocol-buffers/)

## Javascript Data View/Flow/Binding Libraries  (an obscene number of them)

Since our problem is so data-centric, not using data-binding will be a giant pain of always writing new update handlers. 

A data bind and a data flow are related problems, and several of these libraries solve them together. A data _bind_ is when some object (something in-memory or output like a visualization widget or a slider) is marked as being a consumer of some other object, and they are kept in sync. A data _flow_ is a chain of dependent computations; most useful of all is a flow over a series of binds (_think: Microsoft Excel, which lets you edit any non-formula cell and then reupdated all dependent cells automatically; and that is **really, really useful**_)

**Question**: do we support bidirectional binds? Bidirectional binds are much much much harder than unidirectional; the alternative is to manually expose an API for everything modifiable (and this is safer, because is gaurds against accidentally making a model--which is more likely than not written quick-and-dirty--inconsistent)

* [LavaJS](http://lava.codeplex.com/) - _this one is promising; it is small and claims to be unobtrusive
* http://jqxb.codeplex.com/
* [Knockout](http://knockoutjs.com/); 
  * rave reviews [here](http://blog.stevensanderson.com/2010/07/05/introducing-knockout-a-ui-library-for-javascript/) and [here](http://visualstudiomagazine.com/articles/2012/02/01/2-great-javascript-data-binding-libraries.aspx)
  * supports functional definitions of quantities--quantities that get recomputed as the underlying data updates
* [jsViews](https://github.com/BorisMoore/jsviews) seems to be jQuery's official plugin to do this 
* [simpli5](https://github.com/jacwright/simpli5) is a bigger thing, but it [features data-binding](http://jacwright.com/438/javascript-data-binding/). It hasn't had an update in 4 years, though (perhaps jQuery superseded it?). Regardless, we can pick through it (and the others) for ideas.; [its magic](https://github.com/jacwright/simpli5/blob/master/src/binding.js) is mostly done with js's built in ```__lookupSetter__```
* Square's [Crossfilter](http://square.github.io/crossfilter/) - _built for big data_
* NYT's [PourOver](http://nytimes.github.io/pourover/) - _built for big data; has updates+dataflow built in, and a dual functional and procedural interface; no databinds (but we could write a shim to do binding on top..) **the only datastructure involved is Collection (ie a set)** anything requiring a join still needs to be done on the server_
* Miso's [Dataset](http://misoproject.com/dataset/)
* Vega's [Triflow](https://github.com/trifacta/triflow/tree/master/test) - _not actually sure if this is a dataflow library; it seems to too tiny to do anything; maybe it's just clever - nick_
* [AngularJS](https://docs.angularjs.org/guide/databinding) includes databinding as a feature
* [BackboneJS](http://backbonejs.org/)
* [Model.JS](https://github.com/curran/model) (new, but written by someone with extensive d3 experience; he even wrote [an intro to AngularJS](http://curran.github.io/screencasts/introToAngular/exampleViewer/#/)
* It might be possible to get away with no learning databinding, but just being wise in application of [d3's native event API](http://bl.ocks.org/mbostock/5872848)
    * [a writeup on this philosophy](http://ag.svbtle.com/on-d3-components)
    * [one of the links from the above](http://swannodette.github.io/2013/07/31/extracting-processes/), in Clojure+JS, for another perspective
* [BaconJS](http://baconforme.com/) (clear tutorial [here](http://blog.flowdock.com/2013/01/22/functional-reactive-programming-with-bacon-js/))
* [BreezeJS](http://www.breezejs.com/home), a _LINQ-inspired javascript client_. Perhaps overengineered. Abilities depend on its backends (not unlike what Dataset is to SQLAlchemy is to {MySQL, Postgres, Sqlite, ...}) It is these things:
    * an ORM to SQL
        * recently, [an ORM to MongoDB](http://www.breezejs.com/documentation/mongodb)
    * a query language
    * an intelligent cache
    * helper functions for supporting {Knockout, Angular, ...}'s databinding

We have two related but distinct tasks: 1) query 2) binding. The ideal would be finding (or making) an API that can bind queries:

```
// dream query-binding example
init() {

speedsheetwidget = handsontable(...);
barplot = nvd3.barplot(...);
barplot.title("Farmer Income");

feed = ServerDBProxy.tables.agents.where(time=t, agenttype="farmer").select(backaccount);
bind(spreedsheetwidget, feed);
bind(barplot, feed);
}
```
with zero application-layer code handling the updates
(this will definitely not work with what we have now, but it is inspired by SQLAlchemy and LINQ)
It might turn out that there's no sensible way to write query+binding without writing querybinding. At least, not with the current state of javascript. Or something.

(this exists in some form in:
* CouchDB. It is called [replication filtering](http://couchdb.readthedocs.org/en/latest/replication/protocol.html#filter-replication) ([example](http://guide.couchdb.org/draft/notifications.html#filters)) in `feed=continuous` mode)
* Breeze. The components are [projection queries](http://www.breezejs.com/documentation/projection-queries) + [change tracking](http://www.breezejs.com/documentation/change-tracking)
* Don't forget that SQL calls this a [View](https://en.wikipedia.org/wiki/View_%28database%29); there's no way to define a View dynamically, though.
* A pre-Breeze [example](http://msdn.microsoft.com/en-us/magazine/jj133816.aspx) using MsSQL Server + WCF (C# backend) +{Data,Knockout}JS (HTML5 frontend); it even binds two ways)

Related threads:
* [dat#112](https://github.com/maxogden/dat/issues/112)
* [nengo-gui#1](https://github.com/ctn-waterloo/nengo_gui/pull/1)



# Web-Based Code Editors

Examples of isolating

* http://repl.it
* http://ideone.com/
* http://gcc.godbolt.org/#

# Backend


### Servers


* Twisted
* Django?
* flask
  * klein

### Webby stuff:

* Jinja2==2.7.1
* MarkupSafe==0.18
* Werkzeug==0.9.4
* klein==0.2.1

### Utils

* [PIL](http://pillow.readthedocs.org/) for generating and working with rasters

### Video??

* ??? ?? ? ? ?

### Databases


[An Overview of Paradigms](http://www.slideshare.net/slidarko/an-overview-of-data-management-paradigms-relational-document-and-graph-3880059) by the author of Gremlin.

* Tables: most of these are SQL, but some are not
    * Python APIs: [Overview](https://wiki.python.org/moin/DatabaseInterfaces), [SQL](https://wiki.python.org/moin/DbApiModuleComparison) which conform to [PEP 249](http://legacy.python.org/dev/peps/pep-0249/), [ORMs &c](https://wiki.python.org/moin/HigherLevelDatabaseProgramming) of which [SQLAlchemy](http://www.sqlalchemy.org/) is head of the pack.
    * [Dee](http://www.quicksort.co.uk/DeeDoc.html) which overloads python operators to create a superset of SQL directly in Python
    * [Dataset](https://dataset.readthedocs.org/) which wraps SQLAlchemy into something ressembling plain dictionaries; somewhat unfinished; working with them would be profitable.
    * Related: [dat](https://github.com/maxogden/dat), a version control (but not query!) system for tabular/object data.
    * [PyTables](http://www.pytables.org/moin) which eschews SQL in favour of hdf5's optimized idea of what a database is (**This isn't SQL.** where does this fit in??
* Maps (_vector maps look a lot like tables, except with a special set of 'geometry' types; raster maps are a different beast_). generally 
**Map Data** _is complicated_
    * [ArcGIS Server](http://www.esri.com/software/arcgis/arcgisserver/)
    * [PostGIS](http://postgis.net/) for postgres
    * [GeoREST](https://code.google.com/p/georest/)
    * [MapFish](http://trac.mapfish.org/trac/mapfish/wiki/MapFishProtocol), for
    * Formats:
        * TopoJSON
        * GeoJSON
        * (obligatory overengineered XML format: FIXME) [CRUD](https://en.wikipedia.org/wiki/Create,_read,_update_and_delete) operations.
        * Formats [supported by OpenStreetMap](http://wiki.openstreetmap.org/wiki/OSM_file_formats) as available from [planet.osm.org](http://wiki.openstreetmap.org/wiki/Planet.osm#Downloading) -- notable that these formats are homegrown, because OSM started after proprietary geodata had a defacto standard (Arc's .shp) but before Open Geodata had one.
            * There's even [a format for streaming deltas](http://wiki.openstreetmap.org/wiki/OsmChange) 
    * [OpenLayers' Discussion on approaches to giving different results at different zoom levels](https://github.com/openlayers/ol3/pull/1812) (and [related PR](https://github.com/openlayers/ol3/pull/1744)
* Graph / Network Databses
    * [pggraph](http://pgfoundry.org/projects/pggraph) for postgres
    * [Neo4j](https://en.wikipedia.org/wiki/Neo4j) ("the most popular graph database")
    * [Gremlin](https://github.com/tinkerpop/gremlin/wiki); the [author](http://www.slideshare.net/slidarko/) has many talks online:
        * A [Gremlin Overview](http://www.slideshare.net/slidarko/the-pathological-gremlin)
        * [Memoirs of a GraphDB Addict](http://www.slideshare.net/slidarko/memoirs-of-a-graph-addict-despair-to-redemption#)
        * Another [Gremlin Talk](http://www.slideshare.net/slidarko/gremlin-a-graphbased-programming-language-3876581)
        * An [arxiv preprint](http://arxiv.org/abs/1004.1001)
        * [Yet Another Gremlin talk](http://www.slideshare.net/slidarko/the-pathology-of-graph-databases)
* OLAP
    * [Cubes](http://cubes.databrewery.org/) which wraps SQL into OLAP _**pay attention** to this one_
* TimeSeries
    * Square's [cube](http://square.github.io/cube/) 
* NoSQL (aka Document Databases)
    * [MongoDB](http://www.mongodb.org/) - _**NB**: commercial use is a 5000$ license_
    * [CouchDB](http://couchdb.readthedocs.org/)
* Objects (most of these look like blazingly fast dictionaries, since all object oriented programming can be reduced to dictionaries of dictionaries) -- the Document Databases often end up looking identical to object databases
    * [Redis](http://redis.io/)



Remote Query Protocols

* [OData](http://www.odata.org/) - a protocol for exposing (SQL?) RESTfully; seems overengineered.
    * [Query Langauge Spec](http://docs.oasis-open.org/odata/odata/v4.0/os/part2-url-conventions/odata-v4.0-os-part2-url-conventions.html#_Toc372793791) 
* [SparQL](http://www.w3.org/TR/sparql11-query/) - for querying document databases
    * [Example 2](http://www.ibm.com/developerworks/xml/library/j-sparql/) 

## Data Types

* [rfc3339: DateTimes](http://www.ietf.org/rfc/rfc3339.txt)

### Language Bridges

* [RPy](http://rpy.sourceforge.net/rpy2.html) and its child [rmagic](http://ipython.org/ipython-doc/dev/config/extensions/rmagic.html) to hook out
* Jython to wrap java code??

## Patterns

* REST
* Pub/Sub
* Events
* Declarative (e.g. d3)

## Modelling
* Repast
* GarlicSIM
* [SimPy](http://simpy.readthedocs.org/)
* [PyABM](http://www-rohan.sdsu.edu/~zvoleff/research/pyabm/) (canonical usage example [here](https://github.com/azvoleff/chitwanabm/blob/master/chitwanabm/agents.py))
  * check out [what wikipedia thinks](http://en.wikipedia.org/wiki/Comparison_of_agent-based_modeling_software) to be bored to tears
* [ABCE](https://github.com/DavoudTaghawiNejad/abce) _Agent Based Complete Economy_ (python); [paper](http://jasss.soc.surrey.ac.uk/16/3/1.html)
* http://insightmaker.com/
* [Liam2](http://liam2.plan.be/pages/about.html)


## Statistics

This software list has been outsourced to [Statistics.md](Statistics.md).

## Packaging

* [py2exe](http://www.py2exe.org/) - _specialized for Windows, runs as a distutils extension_
* [freeze.py](https://wiki.python.org/moin/Freeze) - _specialized for Unix; not used that much anymore, since most Unices have good package managers these days_
* [PyInstaller](http://pyinstaller.org/)  - _cross-platform python packaging_

(nick thinks we should rely on system-specific packaging conventions as much as possible, e.g. try to get our work into brew, pacman, aptitude and the Windows 8 and OS X App Stores)

## Tools

* [GeoHack](https://tools.wmflabs.org/geohack/) (all lat/lon coordinates on Wikipedia link to GeoHack)
* [MapBox Collaboratory](https://www.mapbox.com/)

## Collaboratories 

Collaboraties (laboratories for collaboration) are our key to reproducible science.

* [StackExchange](http://stackoverflow.com/) - _they've thought long and hard -- and have measured -- about the right way to do comment systems; the link escapes me right now, though -kousu_

* [DIY.org](http://diy.org/)
* [jsFiddle](http://jsfiddle.net) -- [example](http://jsfiddle.net/sharavsambuu/s7QjN/9/light/)
* [Tributary.IO](https://github.com/enjalot/tributary.io): Rapid Collaborative D3 Prototyping
* [SSVD](https://www.icpsr.umich.edu/icpsrweb/ICPSR/ssvd/index.jsp) - _this is both a data archive and a great example of including tools (in this case crosstabs) online and in the browser__

* asana.com -- project management
* github
* academia.edu??
* 

* [nbviewer](http://nbviewer.ipython.org/) which lets scientific people show off their [ipython notebooks](http://ipython.org/notebook.html), like [this one](http://nbviewer.ipython.org/urls/raw2.github.com/damontallen/Orbitals/master/Hydrogen%20Orbitals%20-%20working.ipynb)

### Dataset Management

* http://dat-data.com 
* http://datahub.io
* (there's at least two other dataset-version-control/archival sites; what are they?)
* https://exversion.com
* [FigShare](http://figshare.com/)

### Citation Management

* http://www.zotero.org/
* http://www.mendeley.com/

Plus the tried and true:

* wikis (private or public)
* Dropbox/Google Drive
* .txt files
* .bib files

And the more novel:

* http://rapgenius.com/static/education
* http://a.nnotate.com/
* http://drawhere.com/

## Blogs
* wordpress
* mezzanine (on django)
