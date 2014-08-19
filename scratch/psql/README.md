
A subproject to provide a framework for linking model data to the web in a way that supports sophisticated queries so you can get specific pieces of data, and that keeps data up to date without requiring the web-page to re-download the whole page each time.

Postgres 9.4 has a feature called "Logical Replication", which does most of what we need, but it is not ready for prime time yet.

We are currently thinking of using this with D3 for plots and maps.

Alternative approaches to using data on the web include querying specific streams, CouchDB, and dat (dat-data.com). (What others?)

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

