#!/usr/bin/env bash
# usage: ./client.sh [dbname]
#  launches psql pointed at the db 
#  server.sh must be running for this to work

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
cd $HERE

DB=$1
if [ -z $DB ]; then
  DB="postgres"  # initdb makes a default database called "postgres" so we just ride on that as the default
fi

PGDATA=`pwd`/data

psql -d $DB -h $PGDATA #<-- this construction makes -h start with a slash which makes postgres interpret it as a path instead of an address which means psql connects over unix domain instead of tcp


