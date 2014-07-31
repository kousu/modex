#!/bin/bash


pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
cd $HERE

psql -d postgres -h 127.0.0.1
# -h "$HERE"
 # initdb makes a default database called "postgres" so we just ride on that.
 #  -h makes postgres use the current directory look for its socket in $HERE
 # ...except for some reason "postgres -k ." doesn't seem to actually write the socket file into $HERE,
 #  but it stops postgres from trying to write to /var/run/postgresql
 #so I fall back on TCP, as usual. Which is fine, because that makes sniffing the traffic easier.
