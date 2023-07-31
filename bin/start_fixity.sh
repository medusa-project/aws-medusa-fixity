#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

#Run fixity on restored files in order of restoration completion, run fixity every 5 seconds
for  (( i=1; i <= 3; i++ ))
do
    ruby "$BIN_HOME"/compute_fixity.rb &
    FIXITY_PID=$!
    echo "$FIXITY_PID" > "$TMP_HOME"/fixity."${i}".pid
    sleep 10
done

exit 0