#!/bin/bash --login
source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh
#Run fixity on restored files in order of restoration completion

( cd /home/ec2-user/aws-medusa-fixity || exit; ruby -r "./lib/batch_restore_files.rb" -e "BatchRestoreFiles.get_batch_from_list ['120479', '61138', '663727', '655861', '639857', '69886', '632374', '68656', '50174', '647025', '587873', '69797', '125132', '50633', '50024']" )

exit 0