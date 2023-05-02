#!/bin/bash --login
#fetch responses from the S3 restoration events

( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/restoration_event.rb" -e "RestorationEvent.handle_message" )

exit 0