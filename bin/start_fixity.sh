#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

FIXITY_PID=$$
echo "$FIXITY_PID" > "$TMP_HOME"/start_fixity.pid
#Run fixity on restored files in order of restoration completion, run fixity every 5 seconds
for  (( i=1; i <= 6; i++ ))
do
    ruby "$BIN_HOME"/start_fixity.rb &
    sleep 10
done

exit 0