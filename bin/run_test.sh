#!/bin/bash --login
source ./set-vars.sh

( cd $TEST_HOME || exit; ruby test_${1}.rb --verbose)
exit 0