# Backend Developer's Guide

First review the [generic developer's guide](../README.md).

As a backend developer, it is worthwhile to learn how to run the server directly.
While you can develop portions of it under a simple ```run.py```,
running it directly removes the source of potential bugs from that one layer of indirection
 and lets you tweak command line arguments. To run the server directly, use:
```bash
$ python src/backend/server.py
```

It doesn't matter what folder you run the server from (_if it does seem to matter, file a bug report_)
```bash
$ cd src/backend
$ python server.py
```

If you add 'debug' as an argument to the server you will see the web logs:
```bash
$ cd src/backend
$ python server.py debug
2014-01-19 16:20:19-0500 [-] Log opened.
2014-01-19 16:20:19-0500 [-] Site starting on 8080
2014-01-19 16:20:19-0500 [-] Starting factory <twisted.web.server.Site instance at 0x2e4e680>
2014-01-19 16:20:19-0500 [-] Now open http://127.0.0.1:8080 in your browser
2014-01-19 16:20:20-0500 [HTTPChannelHixie76Aware,0,127.0.0.1] 127.0.0.1 - - [19/Jan/2014:21:20:20 +0000] "GET /assets/ HTTP/1.1" 404 145 "-" "Mozilla/5.0 (X11; Linux x86_64; rv:26.0) Gecko/20100101 Firefox/26.0"
^C2014-01-19 16:20:25-0500 [-] Received SIGINT, shutting down.
2014-01-19 16:20:25-0500 [twisted.web.server.Site] (TCP Port 8080 Closed)
2014-01-19 16:20:25-0500 [twisted.web.server.Site] Stopping factory <twisted.web.server.Site instance at 0x2e4e680>
2014-01-19 16:20:25-0500 [-] Main loop terminated.
```

 (coming soon: extra arguments taking filenames of datalayers to load up)

## Developer's Tools

????
http://phantomjs.org/ might come in handy

SQL GUIs:

* sqliteman
* sqlitebrowser
* phpMyAdmin

## WebSocket Notes

We're using [autobahn](https://github.com/tavendo/AutobahnPython) as our backend implementation of the websocket protocol (it's a protocol that rides on top of HTTP, just to make the web stack even deeper).
WebSockets provide a (**TODO**: summarize and/or link why websockets) more efficient protocol--less bandwidth and much better latency--than typical request-response HTTP. We are using them because we expect (though we don't actually have any).

In Twisted, a "protocol" is a class which knows how to send and receive data in some particular way,
 and gets instantiated anew **for each connection**(!).
 To hook up a "protocol" to a server, you must wrap the protocol in a "ServerFactory" and pass that as a node to the actual server (this is already done and shouldn't have to be tweaked too much).
The tricky part of this for development is that since each 'protocol' happens new for each connection from each client,
you should not build any statefulness into your "protocols" even though they feel in some ways like the heart of the server. You need to make a tertiary class . Think of protocols as _views_ on your _model_ (and the twisted server which holds the listening TCP connections as the _controller_).
 You _can_ remember the position of your database cursor or your file's seek position within a single "protocol", but anything, updating the database needs to be external (and make the assumption that the connection can die at piece).
However, Twisted thought ahead and covered this situation: every instance of a protocol gains, as .factory, the Factory which created it; so any shared state can (and in most of the [examples](http://twistedmatrix.com/documents/current/core/examples/), is) shared there.

* [Server-side WebSocket API reference](https://github.com/tavendo/AutobahnPython/tree/master/doc)
* [Browser-side WebSocket API reference](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket).

WebSockets, like the _webservice_ protocols that came before them, are referenced like any other HTTP or object: with a URL.
A websocket URL looks like "ws://server:port/path/to/endpoint" or "wss://server:port/path/to/endpoint", the difference being that the latter causes the websocket to connect over SSL.

Autobahn is functional, but it's [website](http://autobahn.ws/python/tutorials/echo/) is currently (**TODO**: update this when this changes) out of date. The github docs and examples seem to be reasonably up to date (they have test cases and stuff, so they better be) so for now, use that as your reference.

Here's some guidance on debugging websockets. Debugging them means tweaking things in ```server.py``` and then opening up
a javascript console from a browser. These notes are in this **README** and not **[[../frontend/README.md]]** since this is really more of an issue--but both sides can and should refer and add to these notes.

When you're in the browser console, you can make a websocket to test with by:
```
   ws = new WebSocket("ws://server:9000/endpoint");
```
   If you drop the ```new``, it'll still work. The difference [is](http://trephine.org/t/index.php?title=Understanding_the_JavaScript_new_keyword) and [occasionally non-existent](

Using 'wss' is a good idea, but unless you've got SSL set up on the server it'll hang and you won't know why because neither the browser API nor Autobahn explicitly mention "SSL" when such a connection fails.

The hostname MUST be the same as the site you are on due to the same-origin policy (unless you do some server side black magic which we shall avoid)
 * In particular, when debugging, you should first open up the server in your webbrowser (autobahn gives a useful response if you hit it with http:// instead of ws://, so if your websocket is supposed to be ws://localhost:9242/ws, then first open http://localhost:9242/ws before trying to debug stuff)

This one-liner lets you test if a websocket is listening:
```
  s = new WebSocket("ws://localhost:8080"); s.onmessage = function(d) { console.log("recv: " + d.data); }; setTimeout(function() { s.send("butts")},1000);
```
  (the setTimeout is because the websocket needs a moment to wake up and handshake; better would be 'onopen'... but with this method you get an error instead of a silent an inexplicable hang)

## GIS Notes

"Shapefiles" are actually 4 (or sometimes 5 or 6) separate files all with the same name but different extensions in a folder. However, most tools can deal with the same files zipped up.
```
cd location_of_shapefile
zip myshapefile.zip *
````

```myshapefile.zip``` is now loadable into most GIS tools (try it in Arc or QGIS). The one tool which doesn't like zipped shapefiles is, frustratingly, GDAL. GDAL can handle folder shapefiles though using this definition "a shapefile is a folder with a .shp file inside of it"

## Gotchas


The symlink to `eutopia`  here is a workaround so that our (python) model is importable into our (python) server.
Long term, we'll define some sort of model-running API and define a model-running host program and 
do model runs in subprocesses, which very well might be another language, such as Java (e.g. Repast or NetLogo).

You must run the server under python2. Twisted is [the largest](https://wiki.python.org/moin/Python2orPython3) library yet to be ported to python3.

In twisted, node.putChild() must have its first argument **NOT** end with a '/'. That will cause mysterious 404s with no clue in the log.

* [Here](http://krondo.com/?p=1209)'s a fun (very poetic) book about Twisted.
* [Network programming with the Twisted framework, Part 1](http://www.ibm.com/developerworks/linux/library/l-twist1/index.html)

In OL3, when loading Vector layers, you (for some currently unknown reason) must pass 'renderer: ol.RendererHint.CANVAS' to the Map constructor or else they will fail with a nil ptr exception in the bowels of OL.

New additions regarding loading a simulation:
The file load_sim.py has been added to /src/backend. What this does is import all the needed files from the models folder. All the nasty
work is done here, such as configuration the initial simulation.

In sim_server.py (copied Nick's server.py), you need two lines to get the Simulation object:
import load_sim
sim = load_sim.sim

Then running sim.step() returns a Python dictionary of the next year in the simulation.
Note: Javascript Console is giving me an error about how it doesn't like to be formatted, BUT, the server sends 
it just fine without halting.

What if we moved models inside backend? This reduces a TON of issues. For instance, certain counties number 58-341 (in guatemala.json) 
fail to load for whatever reason when being called from outside the directory. I already altered map.py, yet this problem still persists.

If we make models a subdirectory of backend, we wouldn't have this problem whatsoever. 



## Compatibility

we should at least cover:
Installing the server on
* Ubuntu
* Mac
