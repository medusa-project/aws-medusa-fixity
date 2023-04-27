require 'aws-sdk-s3'
require 'digest'
require 'aws-sdk-dynamodb'

require_relative 'fixity/fixity_constants.rb'

class Fixity
  MEGABYTE = 1024 * 1024

  def self.run_fixity
    #get object info from dynamodb
    begin
      query_resp= FixityConstants::DYNAMODB_CLIENT.query({
        table_name: FixityConstants::TABLE_NAME,
        index_name: FixityConstants::INDEX_NAME,
        limit: 1,
        scan_index_forward: true,
        expression_attribute_values: {
          ":ready" => FixityConstants::TRUE,
        },
        key_condition_expression: "#{FixityConstants::FIXITY_READY} = :ready",
        return_consumed_capacity: "INDEXES"
      })
      return nil if query_resp.items[0].nil?
    rescue StandardError => e
      error_message = "Error querying dynamodb table: #{e.message}"
      FixityConstants::LOGGER.info(error_message)
    end

    s3_key = query_resp.items[0][FixityConstants::S3_KEY]
    file_id = query_resp.items[0][FixityConstants::FILE_ID]
    initial_checksum = query_resp.items[0][FixityConstants::INITIAL_CHECKSUM]
    file_size = query_resp.items[0][FixityConstants::FILE_SIZE]

    #update dynamodb table to remove fixity ready and set fixity status
    begin
      FixityConstants::DYNAMODB_CLIENT.update_item({
        table_name: FixityConstants::TABLE_NAME,
        key: {
          FixityConstants::S3_KEY => s3_key
        },
        expression_attribute_values: {
          ":fixity_status" => FixityConstants::CALCULATING,
          ":timestamp" => Time.now.getutc.iso8601(10)
        },
        update_expression: "SET #{FixityConstants::FIXITY_STATUS} = :fixity_status, "\
                               "#{FixityConstants::LAST_UPDATED} = :timestamp "\
                           "REMOVE #{FixityConstants::FIXITY_READY}"
      })
    rescue StandardError => e
      error_message = "Error updating fixity ready and fixity status before calculating md5 for object #{s3_key} with ID #{file_id}: #{e.message}"
      FixityConstants::LOGGER.info(error_message)
    end
    # stream s3 object through md5 calculation in 16 mb chunks
    # compare with initial md5 checksum and send medusa result via sqs
    md5 = Digest::MD5.new
    download_size_start = 0
    download_size_end = 16*MEGABYTE
    begin
      while download_size_start < file_size
          object_part = FixityConstants::S3_CLIENT.get_object({
              bucket: FixityConstants::BACKUP_BUCKET, # required
              key: s3_key, # required
              range: "bytes=#{download_size_start}-#{download_size_end}"
          })
          md5 << object_part.body.read
          download_size_end = download_size_end+1
          download_size_start = download_size_end
          download_size_end = download_size_end+16*MEGABYTE
      end
    rescue StandardError => e
      error_message = "Error calculating md5 for object #{s3_key} with ID #{file_id}: #{e.message}"
      FixityConstants::LOGGER.info(error_message)
    end
    #compare calculated checksum with initial checksum
    calculated_checksum = md5.hexdigest
    fixity_outcome = (calculated_checksum == initial_checksum) ? FixityConstants::MATCH : FixityConstants::MISMATCH

    #update dynamodb calculated checksum, fixity status, fixity verification
    update_item_resp = FixityConstants::DYNAMODB_CLIENT.update_item({
      table_name: FixityConstants::TABLE_NAME,
      return_consumed_capacity: "INDEXES",
      key: {
        FixityConstants::S3_KEY => s3_key
      },
      expression_attribute_values: {
        ":fixity_status" => FixityConstants::DONE,
        ":fixity_outcome" => fixity_outcome,
        ":calculated_checksum" => calculated_checksum,
        ":timestamp" => Time.now.getutc.iso8601(3)
      },
      update_expression: "SET #{FixityConstants::FIXITY_STATUS} = :fixity_status, "\
                             "#{FixityConstants::FIXITY_OUTCOME} = :fixity_outcome, " \
                             "#{FixityConstants::CALCULATED_CHECKSUM} = :calculated_checksum, " \
                             "#{FixityConstants::LAST_UPDATED} = :timestamp"
    })

    puts "Update Item Response: #{update_item_resp.to_h}"

    # send sqs to medusa with result
    # queue_url = FixityConstants::SQS_CLIENT.get_queue_url(queue_name: FixityConstants::MEDUSA_QUEUE).queue_url
    checksums = {FixityConstants::MD5 => calculated_checksum}
    parameters = {FixityConstants::CHECKSUMS => checksums, FixityConstants::FOUND => FixityConstants::TRUE}
    passthrough = {FixityConstants::CFS_FILE_ID => file_id, FixityConstants::CFS_FILE_CLASS => FixityConstants::CFS_FILE}
    message = {FixityConstants::ACTION => FixityConstants::FILE_FIXITY,
               FixityConstants::STATUS => FixityConstants::SUCCESS,
               # FixityConstants::ERROR_MESSAGE => "error, if any",
               FixityConstants::PARAMETERS => parameters,
               FixityConstants::PASSTHROUGH => passthrough}
    puts message.to_json
    FixityConstants::SQS_CLIENT_EAST.send_message({
      queue_url: queue_url,
      message_body: message.to_json,
      message_attributes: {}
    })
  end
end

