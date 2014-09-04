#!/bin/sh

if [ `uname` = "Darwin" ]; then
  # there's deployment issues on OS X using Postgres.app:
  # http://stackoverflow.com/questions/16407995/psycopg2-image-not-found
  # One of the tips, and this seems the least invasive fix
  # 
  # TODO: figure out a more robust way to deploy on OS X!
  
  export DYLD_LIBRARY_PATH=/Applications/Postgres.app/Contents/Versions/9.3/lib/:$DYLD_LIBRARY_PATH
fi

./replicate.py $@
