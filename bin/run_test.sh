#!/bin/bash --login
source ./set-test-vars.sh

cd $TEST_HOME || exit
test_file="${1}"_test.rb
echo "$test_file"
ruby "$test_file" --verbose
#echo "$outcome"
exit 0