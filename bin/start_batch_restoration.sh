#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

#Run fixity on restored files in order of restoration completion
ruby "$BIN_HOME"/initiate_batch_restoration.rb &
RESTORATION_PID=$!
echo "$RESTORATION_PID" > "$TMP_HOME"/batch_restoration.pid
exit 0