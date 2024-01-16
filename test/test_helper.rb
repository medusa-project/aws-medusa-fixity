# frozen_string_literal: true

require 'simplecov'
SimpleCov.start

require 'config'
require 'csv'
require 'json'
require 'minitest/autorun'

require_relative '../lib/fixity'
require_relative '../lib/fixity/dynamodb'
require_relative '../lib/fixity/fixity_utils'
require_relative '../lib/fixity/s3_control'
require_relative '../lib/fixity/s3'
require_relative '../lib/medusa_sqs'
require_relative '../lib/process_batch_reports'
require_relative '../lib/restoration_event'
require_relative '../lib/process_results'

