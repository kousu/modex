#!/usr/bin/env bash

# this script lets you run postgres without needing root
# it also handles any platform-specific cruft needed to get this system up

# before running this, do
# initdb ./data/
#

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

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
cd $HERE

#"-k ." is the magic that means "put your socket file in your current directory" and not in /var/run which you would need to fight permissions on
postgres -D ./data/  -k .
