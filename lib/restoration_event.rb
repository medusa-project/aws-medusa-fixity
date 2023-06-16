require 'aws-sdk-sqs'
require 'json'
require 'aws-sdk-dynamodb'

require_relative 'fixity/fixity_constants.rb'
require_relative 'fixity/batch_item.rb'
require_relative 'restore_files.rb'

class RestorationEvent
  def self.handle_message
    response = FixityConstants::SQS_CLIENT_WEST.receive_message(queue_url: FixityConstants::S3_QUEUE_URL,
                                                                max_number_of_messages: 10,
                                                                visibility_timeout: 300)
    return nil if response.data.messages.count.zero?
    response.messages.each do |message|
      body = JSON.parse(message.body)
      FixityConstants::SQS_CLIENT_WEST.delete_message({queue_url: FixityConstants::S3_QUEUE_URL,
                                                       receipt_handle: message.receipt_handle})
      records = body["Records"][0]
      restore_type = records["eventName"]
      s3_key = records["s3"]["object"]["key"]
      file_size = records["s3"]["object"]["size"]
      restore_timestamp = records["eventTime"]

      FixityConstants::LOGGER.info("PROCESSING: restore type: #{restore_type}, s3 key: #{s3_key}")

      case restore_type
      when FixityConstants::RESTORE_COMPLETED
        #update dynamodb item to complete, mark fixity ready, and update last updated
        handle_completed(s3_key, file_size, restore_timestamp)
      when FixityConstants::RESTORE_DELETED
        #update dynamodb item to expired, remove fixity ready, and update last updated
        # RestoreFiles.restore_batch(s3_key)
        handle_deleted(s3_key, file_size, restore_timestamp)
      else
        error_message = "Unknown restore type #{restore_type}"
        FixityConstants::LOGGER.error(error_message)
        return nil
      end
    end
  end

  def self.handle_completed(s3_key, file_size, restore_timestamp)
    begin
      FixityConstants::DYNAMODB_CLIENT.update_item({
       table_name: FixityConstants::FIXITY_TABLE_NAME,
       key: {
         FixityConstants::S3_KEY => s3_key
       },
       expression_attribute_values: {
         ":restoration_status" => FixityConstants::COMPLETED,
         ":fixity_ready" => FixityConstants::TRUE,
         ":file_size" => file_size,
         ":timestamp" => restore_timestamp
       },
       update_expression: "SET #{FixityConstants::RESTORATION_STATUS} = :restoration_status, "\
                              "#{FixityConstants::FIXITY_READY} = :fixity_ready, "\
                              "#{FixityConstants::LAST_UPDATED} = :timestamp, "\
                              "#{FixityConstants::FILE_SIZE} = :file_size"
     })
    rescue StandardError => e
      error_message = "Error updating item #{s3_key}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
  end

  def self.handle_deleted(s3_key, file_size, restore_timestamp)
    begin
      update_item_resp = FixityConstants::DYNAMODB_CLIENT.update_item({
        table_name: FixityConstants::FIXITY_TABLE_NAME,
        key: {
          FixityConstants::S3_KEY => s3_key
        },
        expression_attribute_values: {
          ":restoration_status" => FixityConstants::EXPIRED,
          ":file_size" => file_size,
          ":timestamp" => Time.now.getutc.iso8601(3)
        },
        update_expression: "SET #{FixityConstants::RESTORATION_STATUS} = :restoration_status, "\
                               "#{FixityConstants::LAST_UPDATED} = :timestamp, "\
                               "#{FixityConstants::FILE_SIZE} = :file_size "\
                        "REMOVE #{FixityConstants::FIXITY_READY}",
        return_values: "ALL_OLD"
      })
    rescue StandardError => e
      error_message = "Error updating item #{s3_key}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    handle_expiration(update_item_resp)
  end

  #TODO implement for batch processing
  def self.handle_expiration(update_item_resp)
    fixity_status = update_item_resp.attributes[FixityConstants::FIXITY_STATUS]
    #TODO check this logic
    if fixity_status != FixityConstants::DONE && fixity_status != FixityConstants::ERROR
      s3_key = update_item_resp.attributes[FixityConstants::S3_KEY]
      file_id = update_item_resp.attributes[FixityConstants::FILE_ID]
      initial_checksum = update_item_resp.attributes[FixityConstants::INITIAL_CHECKSUM]
      message = "EXPIRATION: File #{file_id} expired before being processed by fixity"
      FixityConstants::LOGGER.info(message)
      item = BatchItem.new(s3_key, file_id, initial_checksum)
      RestoreFiles.restore_batch([item])
    end
  end

end
