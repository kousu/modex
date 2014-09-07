#!/usr/bin/env bash
# usage: source ./pg_vars.sh
#  launches psql pointed at the db 
#  server.sh must be running for this to work

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null

if [ ! -z $1 ]; then
  export PGDATABASE=$1
fi

if [ -z $PGDATABASE ]; then
  export PGDATABASE="postgres"  # initdb makes a default database called "postgres" so we just ride on that as the default
fi

if [ -z $PGDATA ]; then
  export PGDATA=$HERE/data   #<-- this construction makes $PGDATA an absolute path
fi

if [ -z $PGHOST ]; then
  export PGHOST=$PGDATA  #pqsl takes absolute-path PGHOST to mean "use Unix Domain not TCP"
fi

# we don't need to set PGPORT, but be aware that if it is set it changes the name of the listening socket file

