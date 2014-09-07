#!/usr/bin/env bash
# usage: ./client.sh [dbname]
#  launches psql pointed at the db 
#  server.sh must be running for this to work

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null

source $HERE/pg_vars.sh

psql 
