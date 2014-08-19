
This was a project to implement postgres's native [replication protocol](http://www.postgresql.org/docs/current/static/protocol-replication.html), which ships the Write Ahead Logs (WAL) from one postgres to another,
in javascript,
in support of getting filtered replication (see: dat or CouchDB) out of postgres and to the web.
The idea is to implement the postgress replication protocol 

However, after getting as far as being able to receive the WAL logs it was abandoned because the WAL logs turned out to be cryptic and terse not just for efficiency,
but because they actually do not contain the creates, updates and deletes that we need to do the filtering. The WAL logs are, instead,
copies of the binary journal that postgres uses internally, the logs which actually store the database state. This is uns
[TODO: link the postgres mailing list message which finally explained this to me]

* Postgres technical details:

    * [src](http://git.postgresql.org/gitweb/?p=postgresql.git;a=tree)
    * [developer's list](http://www.postgresql.org/list/pgsql-hackers/)
    * [Postgres protocol](http://www.postgresql.org/docs/current/static/protocol.html)
    * [Replication protocol](http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/replication/walsender.c)
    * [WAL definition](http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/include/access/xlog.h) -- in the code, called "XLog" which is short for "Transaction Log"
    * [WAL implementation](http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/access/transam/xlogreader.c)

[websockify](https://github.com/kanaka/websockify) would have helped.

Scrap notes (TODO: move)
------------------------

postgres runs on tcp:5432 (i.e., it streams) reliable in-order delivery, which is important when streaming changes,
but uses a message based (i.e., datagrams) protocol. Also, the protocol is in binary.


