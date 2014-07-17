Experimenting with a variety of ways of watching datastores for changes, in hopes of avoiding having to recompute diffs every step.

* known Approaches:
    * Unix:
        * Linux's [inotify](http://linux.die.net/man/7/inotify)
        * [FAM](http://oss.sgi.com/projects/fam/), which wraps the above depending on platform
            * Gnome's clone of FAM, [GAMIN](https://people.gnome.org/~veillard/gamin/)
        * OS X's [FSEvents](https://developer.apple.com/library/mac/documentation/Darwin/Conceptual/FSEvents_ProgGuide/Introduction/Introduction.html)
        * *BSD's [kqueue](http://www.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2)'s `EVFILT_VNODE` mode can be used to track changes to files; can it also track changes to folders?
            * [stackoverflow on kqueue gotchas](http://stackoverflow.com/questions/15273061/kqueue-tracking-file-changes-chance-of-losing-events-while-processing-previous#15292041)
    * [Postgres](http://www.postgresql.org/docs/current/static/triggers.html)
    * [MySQL](https://dev.mysql.com/doc/refman/5.7/en/triggers.html)
        * MySQL has replication built in. Maybe similar to CouchDB, we can create a fake MySQL server that listens to the replication stream and pushes it up to the web?
    * Could [CouchDB's Replication Protocol](http://couchdb.readthedocs.org/en/latest/replication/protocol.html) in `feed=continous` be used
    * ...?  

