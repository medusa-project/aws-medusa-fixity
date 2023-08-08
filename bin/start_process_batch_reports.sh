#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

#Process batch reports from batch s3 jobs

ruby "$BIN_HOME"/process_batch_reports.rb &
PROCESS_PID=$!
echo "$PROCESS_PID" > "$TMP_HOME"/process_batch_reports.pid
exit 0