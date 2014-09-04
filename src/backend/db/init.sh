#!/usr/bin/env bash
# usage: init.sh dbname
#  constructs a postgres instance, the database "dbname", and installs the replication hooks
# once you run this, run server.sh to use the database



pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
cd $HERE

if [ `uname` = "Darwin" ]; then
  # OS X has several possible python distros
  # our supported configuration uses Postgres.app,
  # which is built linked against the Apple distro of python
  # so we need to ensure the Apple python is the one loaded
  # or else strange library errors will crop up
  #
  # Unfortunately, I'm having trouble building psycopg2 against the system python, so we clients (ie replicate.py) has to be run with Anaconda Python

  export PATH=/usr/bin:$PATH
fi


DB=$1
if [ -z $DB ]; then
  DB="postgres"
fi

export PGDATA=$HERE/data
if [ ! -d $PGDATA ]; then
  initdb
fi

pg_ctl -w start -o "-k ." &&

#<-- this construction makes psql connect over unix domain instead of tcp, since PGDATA is path, not a hostname
psql -d postgres -h $PGDATA <<EOF &&
CREATE DATABASE $DB;
EOF

# install the replication hook
# TODO: do we want to do this installation in replicate.py instead?
psql -d $DB -h $PGDATA < ./replicate.pysql &&

# shutdown
pg_ctl stop
