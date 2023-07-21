#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh

#Run fixity on restored files from csv
( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/fixity.rb" -e "Fixity.run_fixity_from_csv 'manifest-test.csv'" & )
exit 0