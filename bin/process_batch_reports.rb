#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/process_batch_reports'

pbr = ProcessBatchReports.new
pbr.process_failures
