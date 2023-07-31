#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative 'pid'
require_relative 'set_env_vars'
require_relative '../lib/fixity/fixity_constants'

SetEnvVars.set_vars
temp_home = ENV['TMP_HOME']
bin_home = ENV['BIN_HOME']
pid_files = %W[#{temp_home}/fixity.1.pid #{temp_home}/fixity.2.pid #{temp_home}/fixity.3.pid]

pid_files.each do |pid_file|
  pid_id = File.read(pid_file).split.first.to_i
  running = Pid.running?(File.read(pid_file).split.first.to_i)
  FixityConstants::Logger.info("Pid #{pid_id} running?: #{running}")
  next if running

  task = IO.popen("ruby #{bin_home}/compute_fixity.rb")
  File.write(pid_file, task.pid)
  sleep 2
end
