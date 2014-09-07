#!/usr/bin/env bash
# usage:
#  [optional] export PGDATA=... # path to where to the postgres backend data files
#  [optional] export PGHOST=... #path to where to place the listening socket
#  [optional] export PGPORT=11234
#  ./server.sh
#
 
# this script lets you run postgres without needing root
# it also handles any ugly platform-specific cruft
#
# before running this run init.sh

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
source $HERE/pg_vars.sh

if [ `uname` = "Darwin" ]; then
  # OS X has several possible python distros
  # our supported configuration uses Postgres.app,
  # which is built linked against the Apple distro of python
  # so we need to ensure the Apple python is the one loaded
  # or else strange library errors will crop up
  # 
  # But there's different linking errors on the client side, so 
  # this line is in server.sh to keep the invasiveness small.
  
  export PATH=/usr/bin:$PATH 
fi

#'-h ""' disables TCP
#'"-k ."' is the magic that means "put your socket file here and not in /var/run" which you would need to with fight permissions for
postgres -D $PGDATA -h "" -k $PGHOST 
