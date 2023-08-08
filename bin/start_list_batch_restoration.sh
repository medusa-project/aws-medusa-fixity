#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh
#Batch restore files from a list of file ids

ruby "$BIN_HOME"/list_batch_restoration.rb &
RESTORATION_PID=$!
echo "$RESTORATION_PID" > "$TMP_HOME"/list_batch_restoration.pid

exit 0