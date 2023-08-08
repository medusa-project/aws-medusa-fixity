#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh
#Batch restore files from a list of file ids

ruby "$BIN_HOME"/compute_fixity_from_csv.rb &
FIXITY_PID=$!
echo "$FIXITY_PID" > "$TMP_HOME"/fixity_csv.pid

exit 0