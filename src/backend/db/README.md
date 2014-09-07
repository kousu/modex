
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
$ ./init.sh sim1
$ ./server.sh
```

Note: you can also set PGDATABASE instead of providing your database ("sim1" in this case) on the command line.

2) 
$ # In a second terminal
$ ./client.sh sim1 < tests/test.psql #set up some tables for experimenting: curently loads the 'films' table
```

3) run the websocket replication server
```
$ # In a third terminal
$ ./replicate_server.sh 8081 postgresql:///sim1?host=`pwd`/data/ films  
```
The 8081 is important here, since that port number is hardcoded in the javscript demos.
Using `pwd` is important here in order to convince SQLAlchemy to speak to a unix domain socket--which is the only way to speak to the devel postgres that ./server.sh spawns.
This step is the kludgiest part of the system, and will be cleaned up some day so that
you need only run one server script that spawns then reaps all the rest.

4) run the frontend
In the **project root**
```
$ # In a fourth terminal
$ python -m SimpleHTTPServer  #or http.server for python3
```

and go to http://localhost:8000/src/frontend/setgraph.html in your browser and open up the js console to watch the action.
The `DB` object contains the replicated table.

5) Write to the database
Any `INSERT`, `UPDATE` or `DELETE` done on the command line should immediately show up in your js console.
```
# Back in the second terminal
$ ./client.sh sim1
sim1=# insert into films values ('SuaveMan', 'Romantic Comedy', -3);
INSERT 0 1
sim1=# delete from films where rating < 5;
DELETE 6
sim1=# 
```
Pay attention to the frontend as you do this.

You can also run one of the simulations that has been retrofitted with `SimulationLog` and point
 it at postgres (instead of its default of using RAM) using SQLAlchemy connection strings.
If you then rerun the replication server on one of that simulation's tables, the frontend will end up with that instead,
and rerunning the simulation will append to the `DB` object in simulated real time.

To reset, just run
```
$ ./reset.sh
```
 and start at the top.



Issues
------

* Security: exposing SQL operations, even indirectly, has the potential to be dangerous.

Files
-----

TODO


Links
-----

