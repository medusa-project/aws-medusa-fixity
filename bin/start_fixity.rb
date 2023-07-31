#!/bin/bash --login
# frozen_string_literal: true

require_relative 'pid'

system 'source /home/ec2-user/aws-medusa-fixity/bin/set-vars.sh'
temp_home = ENV['TMP_HOME']
bin_home = ENV['BIN_HOME']
pid_files = %W[#{temp_home}/fixity.1.pid #{temp_home}/fixity.2.pid #{temp_home}/fixity.3.pid]

pid_files.each do |pid_file|
  next if Pid.running?(File.read(pid_file).split.first.to_i)

  task = IO.popen("ruby #{bin_home}/compute_fixity.rb")
  File.write(pid_file, task.pid)
  sleep 2
end
