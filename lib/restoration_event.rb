require 'aws-sdk-sqs'
require 'json'
require 'aws-sdk-dynamodb'
require 'config'

require_relative 'fixity/fixity_constants.rb'
require_relative 'fixity/batch_item.rb'
require_relative 'fixity/dynamodb'
require_relative 'batch_restore_files.rb'

class RestorationEvent
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  def self.handle_message
    dynamodb = Dynamodb.new()
    #TODO query for file id? Unsure if necessary or helpful
    response = FixityConstants::SQS_CLIENT_WEST.receive_message(queue_url: Settings.aws.sqs.s3_queue_url,
                                                                max_number_of_messages: 10,
                                                                visibility_timeout: 300)
    return nil if response.data.messages.count.zero?
    response.messages.each do |message|
      body = JSON.parse(message.body)
      FixityConstants::SQS_CLIENT_WEST.delete_message({queue_url: Settings.aws.sqs.s3_queue_url,
                                                       receipt_handle: message.receipt_handle})
      records = body["Records"][0]
      restore_type = records["eventName"]
      s3_key = records["s3"]["object"]["key"]
      file_size = records["s3"]["object"]["size"]
      restore_timestamp = records["eventTime"]

      FixityConstants::LOGGER.info("PROCESSING: restore type: #{restore_type}, s3 key: #{s3_key}")

      case restore_type
      when Settings.aws.dynamo_db.restore_completed
        #update dynamodb item to complete, mark fixity ready, and update last updated
        handle_completed(dynamodb, s3_key, file_size, restore_timestamp)
      when Settings.aws.dynamo_db.restore_deleted
        #update dynamodb item to expired, remove fixity ready, and update last updated
        handle_deleted(dynamodb, s3_key, file_size, restore_timestamp)
      else
        error_message = "Unknown restore type #{restore_type}"
        FixityConstants::LOGGER.error(error_message)
        return nil
      end
    end
  end

  def self.handle_completed(dynamodb, s3_key, file_size, restore_timestamp)
    table_name = Settings.aws.dynamo_db.fixity_table_name
    key = { Settings.aws.dynamo_db.s3_key => s3_key }
    expr_attr_values= {
      ":restoration_status" => Settings.aws.dynamo_db.completed,
      ":fixity_ready" => Settings.aws.dynamo_db.true,
      ":file_size" => file_size,
      ":timestamp" => restore_timestamp
    }
    update_expr = "SET #{Settings.aws.dynamo_db.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamo_db.fixity_ready} = :fixity_ready, "\
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamo_db.file_size} = :file_size"
    dynamodb.update_item(table_name, key, {}, expr_attr_values, update_expr)
  end

  def self.handle_deleted(dynamodb, s3_key, file_size, restore_timestamp)
    table_name = Settings.aws.dynamo_db.fixity_table_name
    key = { Settings.aws.dynamo_db.s3_key => s3_key }
    expr_attr_values = {
      ":restoration_status" => Settings.aws.dynamo_db.expired,
      ":file_size" => file_size,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamo_db.file_size} = :file_size "\
                  "REMOVE #{Settings.aws.dynamo_db.fixity_ready}"
    ret_val =  "ALL_OLD"
    update_item_resp = dynamodb.update_item(table_name, key, {}, expr_attr_values, update_expr, ret_val)
    handle_expiration(dynamodb, update_item_resp)
  end

  #TODO implement for batch processing
  def self.handle_expiration(dynamodb, update_item_resp)
    return nil if update_item_resp.nil?
    fixity_status = update_item_resp.attributes[Settings.aws.dynamo_db.fixity_status]
    #TODO check this logic
    if fixity_status != Settings.aws.dynamo_db.done && fixity_status != Settings.aws.dynamo_db.error
      s3_key = update_item_resp.attributes[Settings.aws.dynamo_db.s3_key]
      file_id = update_item_resp.attributes[Settings.aws.dynamo_db.file_id].to_i
      initial_checksum = update_item_resp.attributes[Settings.aws.dynamo_db.initial_checksum]
      message = "EXPIRATION: File #{file_id} expired before being processed by fixity"
      FixityConstants::LOGGER.info(message)
      item = BatchItem.new(s3_key, file_id, initial_checksum)
      s3 = S3.new
      BatchRestoreFiles.restore_item(dynamodb, s3, item)
    end
  end

end
