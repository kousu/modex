
A subproject to provide a framework for linking model data to the web in a way that supports sophisticated queries so you can get specific pieces of data, and that keeps data up to date without requiring the web-page to re-download the whole page each time.

Postgres 9.4 has a feature called "Logical Replication", which does most of what we need, but it is not ready for prime time yet.

We are currently thinking of using this with D3 for plots and maps.

Alternative approaches to using data on the web include querying specific streams, CouchDB, and dat (dat-data.com). (What others?)

Quickstart
---------

To start, make sure you have installed python, postgresql with the pl/python extension, and 

**warning: these instructions are not copypasteable. you need to think and understand before you use them**

1) set up the database 
```
$ cd modex/scratch/psql
$ initdb data/ #initialize postgres('s data)
$ ./server.sh  #start postgres
$ ./client.sh < replicate.pysql #load the replicate.py hooks into postgres
$ ./client.sh    #open up postgres and set up some tables, e.g. the 'films' table from test.sql
```

2) run the websocket replication server
```
$ cd modex/scratch/psql
$ ./replicate_server.sh 8081 films  #in this case, ws://localhost:8081 will replicate table films
```

3) run the frontend
```
$ cd modex/
$ python -m SimpleHTTPServer  #or http.server for python3
$ firefox http://localhost:8000/src/frontend/pourgraph.html
```
(and open up the js console to watch the action)

4) apply some updates (e.g. the second batch of lines about films from test.sql)
Any `INSERT`, `UPDATE` or `DELETE` done on the command line should immediately show up in your js console.


To reset
```
$ rm -r data/
```
 and start at the top.

Issues
------

* Security: exposing the raw SQL protocol to the web has lots of implicit problems.
  Better idea: flesh out replicant.py until it can speak to postgres, have it reformat the WAL logs into JSON and ship those, read-only. We can even drop Websockify (though it might simply be easier and more reliable to chain a pipe + nc + websockify together) 

Files
-----

* server.sh / client.sh : short bash scripts which launch a fresh Postgres instance in the local directory
* websocket.sh : run the websockify proxy, with automatic SSL cert generation.
* replicant.py : prototype implementation of the replication protocol. This is the main file and it reimplements what we need of http://www.postgresql.org/docs/current/static/protocol-replication.html in Python.
* replicant.js : postgres protocol in Javascript, from what was learned. This does not exist yet and would be a reimplementation of replication.py. It may or may not end up being needed.
* ????.js: shim which does datagram-to-stream reconstruction (since WebSockets, despite running over TCP, do not have a stream mode, which postgres (and many other) protocols assume)

Links
-----

