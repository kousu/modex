#!/usr/bin/env bash
# replicate_server.sh
# DEPENDS: socat(1), python3
# 
# use socat to create something like a minimalistic inetd
# [*a* version of netcat, the one that has -e, could also do this]
# In this case, hardcoded to spawn view.py instances.
# see view.py for the remainder of the protocol after that.
# stick websockify in *front* of this to stick this onto javascript-accessible WebSockets


# the multiple-client effect is achieved by socat TCP-LISTEN,...,fork [nc calls this -k]

PORT="$1"
TABLE="$2"

SERVER="./replicate.py ${TABLE}"

# rather than use up a TCP port for the websockify <--> socat connection
# use a unix domain socket instead
# TODO: name this better / use mkstemp / something
IPC=/tmp/s_websockify_replicate_$table

# start up websockify, daemonized, waiting for connections to proxy to socat
websockify -D $PORT --unix-target=$IPC

# start up socat, proxying TCP to the replication server
# reusaddr gets around lingering (e.g. TIME_WAIT) connections blocking the new bind(),
#  so that you can stop and restart this script immediately
socat UNIX-LISTEN:$IPC,fork,reuseaddr EXEC:"$SERVER"
