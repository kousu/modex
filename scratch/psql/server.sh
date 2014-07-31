#!/bin/bash

# before running this, do
# initdb ./data/
#

pushd $(dirname $0) >/dev/null; HERE=`pwd`; popd >/dev/null
cd $HERE

postgres -D ./data/ -k .
