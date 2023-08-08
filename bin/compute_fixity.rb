#!/usr/bin/env ruby

require_relative '../lib/fixity'
system 'source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh'

fixity = Fixity.new
fixity.run_fixity_batch
