#!/bin/bash --login
#Run fixity on restored files in order of restoration completion

( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/fixity.rb" -e "Fixity.run_fixity" )

exit 0