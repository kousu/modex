#!/bin/sh
# export a table from a sqlite database to csv
# usage:
#  dump.sqlite.sh db.sqlite table_name

DB=$1
TABLE=$2

sqlite3 $DB > "$TABLE.csv" <<EOF
.mode csv
.header on
select * from $TABLE;
EOF
