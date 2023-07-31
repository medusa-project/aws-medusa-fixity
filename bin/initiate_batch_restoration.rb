#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/batch_restore_files'

batch_restoration = BatchRestoreFiles.new
batch_restoration.batch_restore
