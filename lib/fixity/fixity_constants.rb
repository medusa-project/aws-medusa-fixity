# frozen_string_literal: true

require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-s3control'
require 'aws-sdk-sqs'
require 'config'

class FixityConstants
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  # AWS
  S3_CLIENT = Aws::S3::Client.new(region: Settings.aws.region_west)
  SQS_CLIENT_WEST = Aws::SQS::Client.new(region: Settings.aws.region_west, endpoint: "http://localhost:9324",  access_key_id: 'x', secret_access_key: 'x')
  SQS_CLIENT_EAST = Aws::SQS::Client.new(endpoint: "http://localhost:9324")
  DYNAMODB_CLIENT = Aws::DynamoDB::Client.new(endpoint: "http://localhost:8000")
  MEDUSA_QUEUE_URL = "http://localhost:9324/queue/fixity-to-medusa-local"
  S3_QUEUE_URL = "http://localhost:9324/queue/aws-to-fixity-local"
  LOGGER = Logger.new('/Users/gschmitt/workspace/aws-medusa-fixity/logs/fixity.log', 'daily')
  S3_CONTROL_CLIENT = Aws::S3Control::Client.new(region: Settings.aws.region_west)

end
