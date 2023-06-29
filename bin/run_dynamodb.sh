#!/bin/bash --login
source ./set-vars.sh

( cd $RUBY_HOME || exit; ruby -r "./lib/fixity/dynamodb.rb" -e "Dynamodb.test_config")
exit 0