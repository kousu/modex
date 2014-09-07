#!/usr/bin/env bash
# usage: reset.sh
#
# TODO: if user gives PGDATABASE, only wipe that database(?)
# this script is a solution looking for a problem.

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
source $HERE/pg_vars.sh

if [ -d $PGDATA ]; then
  echo "Removing $PGDATA"
  rm -r $PGDATA
fi
