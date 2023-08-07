#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

FIXITY_PID="$TMP_HOME"/start_fixity.pid
kill -9 "$(<"$FIXITY_PID")"