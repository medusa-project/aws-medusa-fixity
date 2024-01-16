#!/usr/bin/env ruby

require_relative '../lib/process_results'
system 'source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh'

proc_results = ProcessResults.new
proc_results.generate_fixity_mismatch_csv
