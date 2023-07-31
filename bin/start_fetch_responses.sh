#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

#fetch responses from the S3 restoration events, check every second
for  (( i=1; i <= 12; i++ ))
do
    ruby "$BIN_HOME"/fetch_responses.rb &
    RESPONSE_PID=$!
    echo "$RESPONSE_PID" > "$TMP_HOME"/fetch_responses."${i}".pid
    sleep 5
done

exit 0