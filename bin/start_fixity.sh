#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

#Run fixity on restored files in order of restoration completion
while :
do
    # test in foreground and in background
    ruby "$BIN_HOME"/start_fixity.rb
done

exit 0