#!/bin/bash --login
#Run fixity on restored files in order of restoration completion

( cd /Users/gschmitt/workspace/fixity || exit; ruby -r "./lib/restore_files.rb" -e "RestoreFiles.get_batch" )

exit 0