#!/bin/sh
# MySQL is picky about paths and its !include support is flakey
#  such that the only safe way to handle it is to use absolute paths everywhere.
#
# This script complies with the hoops necessary to localize and isolate
# MySQL to a particular subdirectory (but without going so far as chrooting).
#
# Do not think of this as a script, think of it as the config file
# and remember that you use mysql --help --verbose to see the options you can set
# or look to https://dev.mysql.com/doc/refman/5.7/en/
# Strangely, not all options (like --user) are available here.
#
# usage: $ mysql.conf.sh PATH > mysql.conf

HERE=$(dirname $0)
. $HERE/util.sh #<-- note the chicken-or-egg problem
HERE=$(abspath $HERE)

SQLDIR=$(abspath $HERE)

cat > $HERE/mysql.conf <<END_OF_MYSQL_CONF

# In this file, you can use all long options that a program supports.
# If you want to know which options a program supports, run the program
# with the "--help" option.

# The following options will be passed to all MariaDB clients
[client]
#password	= your_password
#port		= 3306
socket          = $SQLDIR/mysqld.sock
# this is the database root user, not the unix root user
# (though MariaDB fudges over the distinction when it can)
# mysqld does not have this option since it makes no sense there.
user            = root


# The MariaDB server
[mysqld]

#port		= 3306
# Don't listen on a TCP/IP port at all. This can be a security enhancement,
# if all processes that need to connect to mysqld run on the same host.
# All interaction with mysqld must be made via Unix sockets or named pipes.
# Note that using this option without enabling named pipes on Windows
# (via the "enable-named-pipe" option) will render mysqld useless!
# 
skip-networking

socket          = $SQLDIR/mysqld.sock
tmpdir		= $SQLDIR/tmp/
datadir         = $SQLDIR/data
pid-file        = $SQLDIR/mysqld.pid

# Log to stdout
# this takes precedence over the logfile
# (but ignored if used with mysqld_safe because that script is super sketchy)
console  = TRUE

skip-external-locking
key_buffer_size = 16M
max_allowed_packet = 1M
table_open_cache = 64
sort_buffer_size = 512K
net_buffer_length = 8K
read_buffer_size = 256K
read_rnd_buffer_size = 512K
myisam_sort_buffer_size = 8M



# required unique id between 1 and 2^32 - 1
# defaults to 1 if master-host is not set
# but will not function as a master if omitted
server-id	= 1

END_OF_MYSQL_CONF