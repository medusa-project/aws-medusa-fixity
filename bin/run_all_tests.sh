#!/bin/bash --login
source ./set-test-vars.sh
cd "$RUBY_HOME" || exit
total_runs=0
total_asserts=0
total_fails=0
total_errs=0
total_skips=0
for filename in test/*.rb; do
    outcome=$(ruby "$filename" | tail -1)
    echo "$filename"
    echo "$outcome"
    IFS=" "
    read num_runs runs num_asserts asserts num_fails  fails num_errs  errs num_skips skips<<<"$outcome"
    total_runs=$(( num_runs+total_runs ))
    total_asserts=$(( num_asserts+total_asserts ))
    total_fails=$(( num_fails+total_fails ))
    total_errs=$(( num_errs+total_errs ))
    total_skips=$(( num_skips+total_skips ))
done
echo 'Total:'
echo "$total_runs runs, $total_asserts assertions, $total_fails failures, $total_errs errors, $total_skips skips"
exit 0