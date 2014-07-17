
A subproject to provide a framework for linking model data to the web in a way that supports sophisticated queries so you can get specific pieces of data, and that keeps data up to date without requiring the web-page to re-download the whole page each time.

It implements the postgress replication protocol [the replication protocol](http://www.postgresql.org/docs/current/static/protocol-replication.html). 

We are currently thinking of using this with D3 for plots and maps.

Alternative approaches to using data on the web include querying specific streams, CouchDB, and dat (dat-data.com). What others?



postgres runs on tcp:5432 (tcp for reliable in order delivery)
but uses a message based binary protocol