#!/bin/bash --login
source ./set-test-vars.sh

( cd $TEST_HOME || exit; ruby ${1}_test.rb --verbose)
exit 0