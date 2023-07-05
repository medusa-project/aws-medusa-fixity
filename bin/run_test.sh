#!/bin/bash --login
source ./set-vars.sh

( cd $RUBY_HOME || exit; ruby test/test_${1}.rb --verbose)
exit 0