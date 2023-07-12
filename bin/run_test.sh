#!/bin/bash --login
source ./set-test-vars.sh

cd $TEST_HOME || exit
test_file="${1}"_test.rb
outcome=$(ruby "$test_file" --verbose | tail -1)
echo "$test_file"
echo "$outcome"
exit 0