#!/usr/bin/env ruby
require_relative '../lib/fixity'

`source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh`

fixity = Fixity.new
(1..6).each do |i|
  fixity.get_fixity_batch
  sleep 10
end