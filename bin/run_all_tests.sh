#!/bin/bash --login
source ./set-test-vars.sh
cd $RUBY_HOME || exit
for filename in test/*.rb; do
    echo "$filename"
    ruby "$filename"
done
exit 0