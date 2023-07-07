#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh/set-vars.sh

#Process batch reports from batch s3 jobs

( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/process_batch_reports.rb" -e "ProcessBatchReports.process_failures" )

exit 0