#!/usr/bin/env bash

# the equivalent of sqlite's .dump command
# before running this, do
# initdb ./data/
#

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
cd $HERE

pg_dump -h $HERE/data/ -d postgres
