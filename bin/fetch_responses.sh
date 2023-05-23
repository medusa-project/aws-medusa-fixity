#!/bin/bash --login
#fetch responses from the S3 restoration events, check every second
for  (( i=1; i <= 3; i++ ))
do
    ( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/restoration_event.rb" -e "RestorationEvent.handle_message" & )
    sleep 20
done

#( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/restoration_event.rb" -e "RestorationEvent.handle_message" )
exit 0