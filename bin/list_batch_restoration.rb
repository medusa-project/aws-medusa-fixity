#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/batch_restore_files'

list = [43]
batch_restoration = BatchRestoreFiles.new
batch_restoration.batch_restore_from_list(list)
