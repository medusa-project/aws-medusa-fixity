#!/bin/bash --login
#Run fixity on restored files in order of restoration completion

( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/restore_files.rb" -e "RestoreFiles.get_batch" )

exit 0