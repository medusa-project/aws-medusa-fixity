require 'aws-sdk-sqs'
require 'json'
require 'aws-sdk-dynamodb'

require_relative 'fixity/fixity_constants.rb'
require_relative 'fixity/medusa_item.rb'
require_relative 'restore_files.rb'

class RestorationEvent
  def self.handle_message
    # response = FixityConstants::SQS_CLIENT_WEST.receive_message(queue_url: FixityConstants::S3_QUEUE_URL, max_number_of_messages: 1)
    # return nil if response.data.messages.count.zero?
    #
    # message = JSON.parse(response.data.messages[0].body)
    # FixityConstants::LOGGER.info("SQS response: #{message}")
    #
    # FixityConstants::SQS_CLIENT_WEST.delete_message({queue_url: FixityConstants::S3_QUEUE_URL, receipt_handle: response.data.messages[0].receipt_handle})
    # records = message["Records"][0]
    # restore_type = records["eventName"]
    # s3_key = records["s3"]["object"]["key"]
    # file_size = records["s3"]["object"]["size"]
    # restore_timestamp = records["eventTime"]
    #
    # FixityConstants::LOGGER.info("restoreType: #{restore_type}")
    # FixityConstants::LOGGER.info("s3Key: #{s3_key}")
    # FixityConstants::LOGGER.info("restoreTimestamp: #{restore_timestamp}")
    ###### TEST VALUES #########
    s3_key = "test-key"
    restore_type = FixityConstants::RESTORE_COMPLETED
    file_size = 0
    restore_timestamp = Time.now.getutc.iso8601(3)
    ###### TEST VALUES #########

    case restore_type
    when FixityConstants::RESTORE_POSTED
      #update dynamodb item to initiated and update last updated
      #update dynamodb table with restoration status
      begin
        FixityConstants::DYNAMODB_CLIENT.update_item({
          table_name: FixityConstants::TABLE_NAME,
          key: {
            FixityConstants::S3_KEY => s3_key
          },
          expression_attribute_values: {
            ":restoration_status" => FixityConstants::INITIATED,
            ":file_size" => file_size,
            ":timestamp" => restore_timestamp
          },
          update_expression: "SET #{FixityConstants::RESTORATION_STATUS}  = :restoration_status, "\
                                 "#{FixityConstants::LAST_UPDATED} = :timestamp, "\
                                 "#{FixityConstants::FILE_SIZE} = :file_size"
          })
      rescue StandardError => e
        error_message = "Error updating item #{s3_key}: #{e.message}"
        FixityConstants::LOGGER.info(error_message)
      end
    when FixityConstants::RESTORE_COMPLETED
      #update dynamodb item to complete, mark fixity ready, and update last updated
      begin
        FixityConstants::DYNAMODB_CLIENT.update_item({
          table_name: FixityConstants::TABLE_NAME,
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
        FixityConstants::LOGGER.info(error_message)
      end
    when FixityConstants::RESTORE_DELETED
      #update dynamodb item to expired, remove fixity ready, and update last updated
      # RestoreFiles.restore_batch(s3_key)
      begin
        update_item_resp = FixityConstants::DYNAMODB_CLIENT.update_item({
          table_name: FixityConstants::TABLE_NAME,
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
          return_values: "ALL_NEW"
        })
      rescue StandardError => e
        error_message = "Error updating item #{s3_key}: #{e.message}"
        FixityConstants::LOGGER.info(error_message)
      end
      fixity_status = update_item_resp.attributes[FixityConstants::FIXITY_STATUS]
      if fixity_status != FixityConstants::DONE
        s3_key = update_item_resp.attributes[FixityConstants::S3_KEY]
        file_id = update_item_resp.attributes[FixityConstants::FILE_ID]
        initial_checksum = update_item_resp.attributes[FixityConstants::INITIAL_CHECKSUM]
        item = MedusaItem.new(s3_key, file_id, initial_checksum)
        RestoreFiles.restore_batch([item])
      end
    else
      error_message = "Unknown restore type #{restore_type}"
      FixityConstants::LOGGER.info(error_message)
      return nil
    end
  end
end
