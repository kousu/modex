
A subproject to provide a framework for linking model data to the web in a way that supports sophisticated queries so you can get specific pieces of data, and that keeps data up to date without requiring the web-page to re-download the whole page each time.

The idea is to implement the postgress replication protocol [the replication protocol](http://www.postgresql.org/docs/current/static/protocol-replication.html) in javascript, and use [websockify](https://github.com/kanaka/websockify) to proxy javascript to the database.

We are currently thinking of using this with D3 for plots and maps.

Alternative approaches to using data on the web include querying specific streams, CouchDB, and dat (dat-data.com). (What others?)

Issues
------

* Security: exposing the raw SQL protocol to the web has lots of implicit problems.

Files
-----

* server.sh / client.sh : short bash scripts which launch a fresh Postgres instance in the local directory
* websocket.sh : run the websockify proxy, with automatic SSL cert generation.
* replicant.py : prototype implementation of the replication protocol
* replicant.js : postgres protocol in Javascript, from what was learned
* ????.js: shim which does datagram-to-stream reconstruction (since WebSockets, despite running over TCP, do not have a stream mode, which postgres (and many other) protocols assume)

Links
-----

* [postgres developer's list](http://www.postgresql.org/list/pgsql-hackers/)
* [postgres protocol](http://www.postgresql.org/docs/current/static/protocol.html)

Scrap notes (TODO: move)
------------------------

postgres runs on tcp:5432 (tcp for reliable in order delivery)
but uses a message based binary protocol