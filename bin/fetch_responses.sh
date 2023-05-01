#!/bin/bash --login
#fetch responses from the S3 restoration events

( cd /Users/gschmitt/workspace/aws-medusa-fixity || exit; ruby -r "./lib/restoration_event.rb" -e "RestorationEvent.handle_message" )

exit 0