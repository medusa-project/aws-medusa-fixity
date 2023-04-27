#!/bin/bash --login
#Run fixity on restored files in order of restoration completion

( cd /Users/gschmitt/workspace/fixity || exit; ruby -r "./lib/fixity.rb" -e "Fixity.run_fixity" )

exit 0