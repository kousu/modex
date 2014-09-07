#!/usr/bin/env bash
# usage: init.sh dbname
#
#  constructs a postgres instance, the database "dbname", and installs the replication hooks
# once you run this, run server.sh to use the database

# TODO: figure out how to chain the steps in this script so that the hooks are not installed if the CREATE DATABASE failed

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
source $HERE/pg_vars.sh

if [ `uname` = "Darwin" ]; then
  # OS X has several possible python distros
  # our supported configuration uses Postgres.app,
  # which is built linked against the Apple distro of python
  # so we need to ensure the Apple python is the one loaded
  # or else strange library errors will crop up
  #
  # Unfortunately, I'm having trouble building psycopg2 against the system python, so we clients (ie replicate.py) has to be run with Anaconda Python
  #
  # XXX this was copypasted from server.sh for the case where we need to spawn a server
  #  it would be better if we could instead call server.sh--except we (purposely) don't have daemon control of server.sh

  export PATH=/usr/bin:$PATH
fi


if [ ! -d $PGDATA ]; then
  initdb
fi

if pg_ctl status 2>&1 >/dev/null; then
  ALREADY_RUNNING=1
fi

if [ ! $ALREADY_RUNNING ]; then
  pg_ctl -w start -o "-k $PGHOST"
fi

if [ x"$PGDATABASE" != x"postgres" ]; then  #we know postgres makes the "postgres" database by default, so don't try to CREATE it again
  psql -d postgres -h $PGDATA <<EOF
CREATE DATABASE $PGDATABASE;
EOF
fi

# install the replication hook
# TODO: do we want to do this installation in replicate.py instead?
psql < ./replicate.pysql &&

if [ ! $ALREADY_RUNNING ]; then
  # shutdown
  pg_ctl stop
fi
