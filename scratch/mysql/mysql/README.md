Isolated MySQL using Unix named pipes
======================================

Run `./server.sh` to start up a fresh mysqld
with listening socket `./mysqld.sock`
and the correct `mysql.conf` needed to use it.

Once the server is up, you can kill it by running
`shutdown` (or just `pkill mysqld` if your system has pkill installed).

You can access the MySQL REPL by running `./client`,
and you can dump SQL code into the database by piping into that:
```
$ gunzip -C my_database_dump.sql.gz | ./client
```
`./client` is a thin wrapper around `mysql`, so read that program's
[manpage](FIXME) to see other command line arguments.

The current setup ignores the advice MySQL gives
at boot time by not setting a root password.
This means that the `client` script by default
 a) is root 
 b) has full permissions.
If you set a root password, you must also set up `./client.sh` to know it,
either by passing it on the command line:
```
$ ./client -u root -pmyrootpassword
```
or by editing `./mysql.conf` to cache the password.

TODO
-----

* [ ] Force some ctrl-c into server.sh