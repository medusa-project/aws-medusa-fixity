#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative 'pid'
require_relative '../lib/fixity/fixity_constants'

temp_home = ENV['TMP_HOME']
bin_home = ENV['BIN_HOME']

pid_files = %W[#{temp_home}/fixity.1.pid #{temp_home}/fixity.2.pid #{temp_home}/fixity.3.pid #{temp_home}/fixity.4.pid
               #{temp_home}/fixity.5.pid #{temp_home}/fixity.6.pid #{temp_home}/fixity.7.pid #{temp_home}/fixity.8.pid
               #{temp_home}/fixity.9.pid #{temp_home}/fixity.10.pid #{temp_home}/fixity.11.pid #{temp_home}/fixity.12.pid]
#              #{temp_home}/fixity.13.pid #{temp_home}/fixity.14.pid #{temp_home}/fixity.15.pid #{temp_home}/fixity.16.pid]
batch_restore_pid = "#{temp_home}/batch_restoration.pid"
batch_restoration_running = Pid.running?(File.read(batch_restore_pid).split.first.to_i)
sleep 60 and exit if batch_restoration_running

pid_files.each do |pid_file|
  running = Pid.running?(File.read(pid_file).split.first.to_i)
  next if running

  task = IO.popen("ruby #{bin_home}/compute_fixity.rb")
  File.write(pid_file, task.pid)
  # add in quick sleep to prevent the same file being run through fixity multiple times
  sleep 0.75
end
