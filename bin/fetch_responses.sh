#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh/set-vars.sh

#fetch responses from the S3 restoration events, check every second
for  (( i=1; i <= 12; i++ ))
do
    ( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/restoration_event.rb" -e "RestorationEvent.handle_message" & )
    sleep 5
done

#( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/restoration_event.rb" -e "RestorationEvent.handle_message" )
exit 0