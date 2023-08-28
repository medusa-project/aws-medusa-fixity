require 'aws-sdk-sqs'
require 'json'
require 'aws-sdk-dynamodb'
require 'config'

require_relative 'fixity/fixity_constants'
require_relative 'fixity/fixity_utils'
require_relative 'fixity/batch_item'
require_relative 'fixity/dynamodb'
require_relative 'fixity/s3'
require_relative 'batch_restore_files'

class RestorationEvent
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :s3, :dynamodb, :sqs

  def initialize(s3 = S3.new, dynamodb = Dynamodb.new, sqs = FixityConstants::SQS_CLIENT_WEST)
    @s3 = s3
    @dynamodb = dynamodb
    @sqs = sqs
  end

  def handle_message
    response = @sqs.receive_message({ queue_url: Settings.aws.sqs.s3_queue_url,
                                      max_number_of_messages: 10,
                                      visibility_timeout: 300 })
    return nil if response.data.messages.count.zero?

    response.messages.each do |message|
      body = JSON.parse(message.body)
      @sqs.delete_message({ queue_url: Settings.aws.sqs.s3_queue_url,
                            receipt_handle: message.receipt_handle })
      records = body['Records'][0]
      restore_type = records['eventName']
      s3_key = records['s3']['object']['key']
      file_size = records['s3']['object']['size']
      restore_timestamp = records['eventTime']

      FixityConstants::LOGGER.info("PROCESSING: restore type: #{restore_type}, s3 key: #{s3_key}")

      case restore_type
      when Settings.aws.s3.restore_completed
        # update dynamodb item to complete, mark fixity ready, and update last updated
        handle_completed(s3_key, file_size, restore_timestamp)
      when Settings.aws.s3.restore_deleted
        # update dynamodb item to expired, remove fixity ready, and update last updated
        handle_deleted(s3_key, file_size)
      else
        error_message = "Unknown restore type #{restore_type}"
        FixityConstants::LOGGER.error(error_message)
        return nil
      end
    end
  end

  def handle_completed(s3_key, file_size, restore_timestamp)
    table_name = Settings.aws.dynamodb.fixity_table_name
    key = { Settings.aws.dynamodb.s3_key => s3_key }
    expr_attr_values = {
      ':restoration_status' => Settings.aws.dynamodb.completed,
      ':fixity_ready' => Settings.aws.dynamodb.true,
      ':file_size' => file_size,
      ':timestamp' => restore_timestamp
    }
    update_expr = "SET #{Settings.aws.dynamodb.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamodb.fixity_ready} = :fixity_ready, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamodb.file_size} = :file_size"
    @dynamodb.update_item(table_name, key, expr_attr_values, update_expr)
  end

  def handle_deleted(s3_key, file_size)
    table_name = Settings.aws.dynamodb.fixity_table_name
    key = { Settings.aws.dynamodb.s3_key => s3_key }
    expr_attr_values = {
      ':restoration_status' => Settings.aws.dynamodb.expired,
      ':file_size' => file_size,
      ':timestamp' => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamodb.file_size} = :file_size "\
                  "REMOVE #{Settings.aws.dynamodb.fixity_ready}"
    ret_val = 'ALL_OLD'
    update_item_resp = @dynamodb.update_item(table_name, key, expr_attr_values, update_expr, ret_val)
    handle_expiration(update_item_resp)
  end

  # TODO: implement for batch processing
  def handle_expiration(update_item_resp)
    return nil if update_item_resp.nil?

    fixity_status = update_item_resp.attributes[Settings.aws.dynamodb.fixity_status]
    return false unless fixity_status != Settings.aws.dynamodb.done && fixity_status != Settings.aws.dynamodb.error

    s3_key = update_item_resp.attributes[Settings.aws.dynamodb.s3_key]
    file_id = update_item_resp.attributes[Settings.aws.dynamodb.file_id].to_i
    initial_checksum = update_item_resp.attributes[Settings.aws.dynamodb.initial_checksum]
    message = "EXPIRATION: File #{file_id} expired before being processed by fixity"
    FixityConstants::LOGGER.info(message)
    item = BatchItem.new(s3_key, file_id, initial_checksum)
    batch_restore_files = BatchRestoreFiles.new(@s3, @dynamodb)
    batch_restore_files.restore_item(item)
  end
end
