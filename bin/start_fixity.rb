#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative 'pid'
require_relative '../lib/fixity/fixity_constants'
require_relative '../lib/fixity/fixity_utils'

temp_home = ENV['TMP_HOME']
bin_home = ENV['BIN_HOME']
pid_files = %W[#{temp_home}/fixity.1.pid #{temp_home}/fixity.2.pid #{temp_home}/fixity.3.pid #{temp_home}/fixity.4.pid
               #{temp_home}/fixity.5.pid #{temp_home}/fixity.6.pid #{temp_home}/fixity.7.pid #{temp_home}/fixity.8.pid]

pid_files.each do |pid_file|
  next if FixityUtils.get_fixity_count.zero?

  running = Pid.running?(File.read(pid_file).split.first.to_i)
  next if running

  task = IO.popen("ruby #{bin_home}/compute_fixity.rb")
  File.write(pid_file, task.pid)
end
