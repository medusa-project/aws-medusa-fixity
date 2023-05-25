require 'aws-sdk-sqs'
require 'json'
require 'aws-sdk-dynamodb'

require_relative 'fixity/fixity_constants.rb'
require_relative 'fixity/medusa_item.rb'
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

      ###### TEST VALUES #########
      # s3_key = "156/182/DOI-10-5072-fk2idbdev-1660571_v1/dataset_files/Candidate_FRC_PTAC_Meeting_Summary_Template.docx"
      # restore_type = FixityConstants::RESTORE_DELETED
      ###### TEST VALUES #########

      case restore_type
      when FixityConstants::RESTORE_COMPLETED
        #update dynamodb item to complete, mark fixity ready, and update last updated
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
      when FixityConstants::RESTORE_DELETED
        #update dynamodb item to expired, remove fixity ready, and update last updated
        # RestoreFiles.restore_batch(s3_key)
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
        fixity_status = update_item_resp.attributes[FixityConstants::FIXITY_STATUS]
        if fixity_status != FixityConstants::DONE
          s3_key = update_item_resp.attributes[FixityConstants::S3_KEY]
          file_id = update_item_resp.attributes[FixityConstants::FILE_ID]
          initial_checksum = update_item_resp.attributes[FixityConstants::INITIAL_CHECKSUM]
          error_message = "File #{file_id} expired before being processed by fixity"
          FixityConstants::LOGGER.error(update_item_resp.attributes)
          FixityConstants::LOGGER.error(error_message)
          countFile = File.open("expirationCount.txt")
          count= countFile.read.to_i
          countFile.close
          File.write("expirationCount.txt", count+1)
          item = MedusaItem.new(s3_key, file_id, initial_checksum)
          # RestoreFiles.restore_batch([item])
        end
      else
        error_message = "Unknown restore type #{restore_type}"
        FixityConstants::LOGGER.error(error_message)
        return nil
      end
    end
  end
end
