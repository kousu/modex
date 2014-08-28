#!/usr/bin/env bash
# view_server.sh
# DEPENDS: socat(1), python3
# 
# use socat to create something like a minimalistic inetd
# [*a* version of netcat, the one that has -e, could also do this]
# In this case, hardcoded to spawn view.py instances.
# see view.py for the remainder of the protocol after that.
# stick websockify in *front* of this to stick this onto javascript-accessible WebSockets


# the multiple-client effect is achieved by socat TCP-LISTEN,...,fork [nc calls this -k]

PORT=8082
SERVER="./replicate.py $1"

# reusaddr gets around lingering (e.g. TIME_WAIT) connections blocking the new bind(),
#  so that you can stop and restart this script immediately
socat TCP-LISTEN:$PORT,fork,reuseaddr EXEC:"$SERVER"
