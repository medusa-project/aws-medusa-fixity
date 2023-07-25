# frozen_string_literal: true
require 'aws-sdk-s3'
require 'config'
require_relative 'fixity_constants'
require_relative 'fixity_utils'
require_relative 'dynamodb'
require_relative '../medusa_sqs'

class S3
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :s3_client, :s3_resource

  def initialize(s3_client = FixityConstants::S3_CLIENT)
    @s3_client = s3_client
  end

  def put_object(body, bucket, key)
    begin
      s3_resp = @s3_client.put_object({
        body: body,
        bucket: bucket,
        key: key,
      })
    rescue StandardError => e
      error_message = "Error putting object with key: #{key} in bucket #{bucket}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    return s3_resp
  end

  def get_object_to_response_target(bucket, key, response_target)
    begin
      @s3_client.get_object({
        bucket: bucket, # required
        key: key, # required
        response_target: response_target
      })
    rescue StandardError => e
      error_message = "Error getting object with key: #{key}, response target #{response_target} from bucket #{bucket}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
  end

  def get_object(bucket, key)
    begin
      object = @s3_client.get_object({
        bucket: bucket, # required
        key: key, # required
      })
    rescue StandardError => e
      error_message = "Error getting object with key: #{key} from bucket #{bucket}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    return object
  end

  def get_object_with_byte_range(bucket, key, range)
    begin
      object_part = @s3_client.get_object({
        bucket: bucket, # required
        key: key, # required
        range: range
      })
    rescue StandardError => e
      error_message = "Error getting object using byte_range for key: #{key} from bucket #{bucket}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    return object_part
  end

  def restore_object(dynamodb, bucket, key, file_id)
    begin
      @s3_client.restore_object({
        bucket: bucket,
        key: key,
        restore_request: {
          days: 1,
          glacier_job_parameters: {
            tier: Settings.aws.s3.bulk
          },
        },
      })
    rescue StandardError => e
      # Error requesting object restoration, add to dynamodb table for retry?
      # Send error message to medusa
      error_message = "Error restoring object with key: #{key} from bucket #{bucket}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      item = {
        Settings.aws.dynamodb.s3_key => key,
        Settings.aws.dynamodb.file_id => file_id,
        Settings.aws.dynamodb.message => e.inspect,
        Settings.aws.dynamodb.last_updated => Time.now.getutc.iso8601(3)
      }
      dynamodb.put_item(Settings.aws.dynamodb.restoration_errors_table_name, item)
    end
  end

  def found?(bucket, key, s3_resource = Aws::S3::Resource.new(client: @s3_client))
    key = FixityUtils.unescape(key)
    s3_resource.bucket(bucket).object(key).exists?
  end
end
