#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/batch_restore_files'

manifest = 'manifest-2023-10-27-00:00.csv' # manifest-2023-10-28-00:00.csv
batch_restoration = BatchRestoreFiles.new
# batch_restoration.batch_restore
etag = batch_restoration.put_manifest(manifest)
batch_restoration.send_batch_job(manifest, etag)
