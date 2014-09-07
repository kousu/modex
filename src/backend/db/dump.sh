#!/usr/bin/env bash

# the equivalent of sqlite's .dump command
# before running this, do
# initdb ./data/
#

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
source $HERE/pg_vars.sh

pg_dump
