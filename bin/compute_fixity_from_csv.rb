#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/fixity'
system 'source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh'

csv = 'manifest-test.csv'

fixity = Fixity.new
fixity.run_fixity_from_csv(csv)
