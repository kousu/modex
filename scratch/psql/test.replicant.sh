#!/usr/bin/env sh
# demonstrate how to run the replicant

echo '{"table": "films"}' | ./replicant.py 2>/dev/null

# in future, this will probably be:
#./replicant.py "films" 2>/dev/null
