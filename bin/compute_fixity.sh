#!/bin/bash --login
#Run fixity on restored files in order of restoration completion, run fixity every 5 seconds
#for  (( i=1; i <= 60; i++ ))
#do
#    ( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/fixity.rb" -e "Fixity.run_fixity" & )
#    sleep 1
#done

( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/fixity.rb" -e "Fixity.run_fixity" )
exit 0