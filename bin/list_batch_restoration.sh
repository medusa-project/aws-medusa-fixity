#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh
#Run fixity on restored files in order of restoration completion

( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/batch_restore_files.rb" -e "BatchRestoreFiles.get_batch_restore_from_list ['120479', '69837', '69797', '78804', '75896']" )

exit 0