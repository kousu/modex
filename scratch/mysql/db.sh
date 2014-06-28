#!/bin/sh 
# Construct a fresh MySQL database instance of the (user-account pruned) sluce.wici.ca database

mysql/server & #<-- this is backgrounded

sleep 2 # wait for MySQl to come up (??? is there some way to ask "WHEN ITS READY"?)

#the password used here is irrelevant since the root user has a blank password
mysql/import cmombour_slucew passy5ord cmombour_sluceiidb.sql.gz && 

echo "MySQL SluceII db import complete; database accessible at mysql://127.0.0.1:3306" &&
echo "Use ctrl-c to exit" 

# this funny section is to work around MySQL not responding to Ctrl-C
# because it doesn't, we trap ctrl-c ("INT") and send a shutdown command to MySQL
# ourselves (mysql/shutdown is a thin wrapper which does this)
function do_shutdown() {
  mysql/shutdown
  trap - INT #unlink the event handler
  kill $$    #so that we can kill ourselves ($$ = our PID)
}
trap do_shutdown INT



# spin, waiting for ctrl-c to trigger the INT handler
while true; do sleep 10; done