#!/bin/bash --login
source ./set-vars.sh
cd $RUBY_HOME
for filename in test/*.rb; do
    echo "$filename"
    ruby "$filename"
done
exit 0