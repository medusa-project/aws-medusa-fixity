require 'aws-sdk-s3'
require 'digest'
require 'aws-sdk-dynamodb'
require 'cgi'

require_relative 'fixity/fixity_constants.rb'
require_relative 'send_message.rb'

class Fixity
  MEGABYTE = 1024 * 1024

  def self.run_fixity
    #get object info from dynamodb
    fixity_item = get_fixity_item

    s3_key = fixity_item[FixityConstants::S3_KEY]
    file_id = fixity_item[FixityConstants::FILE_ID].to_i
    initial_checksum = fixity_item[FixityConstants::INITIAL_CHECKSUM]
    file_size = fixity_item[FixityConstants::FILE_SIZE]

    message = "FIXITY: File id #{file_id}, S3 key #{s3_key}"
    FixityConstants::LOGGER.info(message)

    #update dynamodb table to remove fixity ready and set fixity status
    update_fixity_ready(s3_key)

    #compare calculated checksum with initial checksum
    calculated_checksum = calculate_checksum(s3_key, file_id, file_size)

    fixity_outcome = (calculated_checksum == initial_checksum) ? FixityConstants::MATCH : FixityConstants::MISMATCH

    case fixity_outcome
    when FixityConstants::MATCH
      #update dynamodb calculated checksum, fixity status, fixity verification
      update_fixity_match(s3_key, calculated_checksum)
    when FixityConstants::MISMATCH
      #update dynamodb mismatch, calculated checksum, fixity status, fixity verification
      update_fixity_mismatch(s3_key, calculated_checksum)
    else
      outcome_message = "Fixity outcome not recognized"
      FixityConstants::LOGGER.error(outcome_message)
    end

    # send sqs to medusa with result
    #SendMessage.send_message(file_id, calculated_checksum, FixityConstants::TRUE, FixityConstants::SUCCESS, nil )
  end

  def self.get_fixity_item
    #TODO expand to query multiple fixity items at a time
    begin
      query_resp = FixityConstants::DYNAMODB_CLIENT.query({
        table_name: FixityConstants::FIXITY_TABLE_NAME,
        index_name: FixityConstants::INDEX_NAME,
        limit: 1,
        scan_index_forward: true,
        expression_attribute_values: {
          ":ready" => FixityConstants::TRUE,
        },
        key_condition_expression: "#{FixityConstants::FIXITY_READY} = :ready",
      })
      return nil if query_resp.items[0].nil?
    rescue StandardError => e
      error_message = "Error querying dynamodb table: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    query_resp.items[0]
  end

  def self.update_fixity_ready(s3_key)
    begin
      FixityConstants::DYNAMODB_CLIENT.update_item({
       table_name: FixityConstants::FIXITY_TABLE_NAME,
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
      FixityConstants::LOGGER.error(error_message)
    end
  end

  def self.calculate_checksum(s3_key, file_id, file_size)
    # stream s3 object through md5 calculation in 16 mb chunks
    # compare with initial md5 checksum and send medusa result via sqs
    md5 = Digest::MD5.new
    download_size_start = 0
    download_size_end = 16*MEGABYTE
    begin
      while download_size_start < file_size
        object_part = FixityConstants::S3_CLIENT.get_object({
          bucket: FixityConstants::BACKUP_BUCKET, # required
          key: CGI.unescape(s3_key), # required
          range: "bytes=#{download_size_start}-#{download_size_end}"
        })
        md5 << object_part.body.read
        download_size_end = download_size_end+1
        download_size_start = download_size_end
        download_size_end = download_size_end+16*MEGABYTE
      end
    rescue StandardError => e
      error_message = "Error calculating md5 for object #{s3_key} with ID #{file_id}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      update_fixity_error(s3_key)
      exit
    end
    md5.hexdigest
  end

  def self.update_fixity_match(s3_key, calculated_checksum)
    FixityConstants::DYNAMODB_CLIENT.update_item({
      table_name: FixityConstants::FIXITY_TABLE_NAME,
      key: {
        FixityConstants::S3_KEY => s3_key
      },
      expression_attribute_values: {
       ":fixity_status" => FixityConstants::DONE,
       ":fixity_outcome" => FixityConstants::MATCH,
       ":calculated_checksum" => calculated_checksum,
       ":timestamp" => Time.now.getutc.iso8601(3)
      },
      update_expression: "SET #{FixityConstants::FIXITY_STATUS} = :fixity_status, "\
                             "#{FixityConstants::FIXITY_OUTCOME} = :fixity_outcome, " \
                             "#{FixityConstants::CALCULATED_CHECKSUM} = :calculated_checksum, " \
                             "#{FixityConstants::LAST_UPDATED} = :timestamp"
    })
  end

  def self.update_fixity_mismatch(s3_key, calculated_checksum)
    FixityConstants::DYNAMODB_CLIENT.update_item({
      table_name: FixityConstants::FIXITY_TABLE_NAME,
      key: {
        FixityConstants::S3_KEY => s3_key
      },
      expression_attribute_values: {
       ":mismatch" => FixityConstants::TRUE,
       ":fixity_status" => FixityConstants::DONE,
       ":fixity_outcome" => FixityConstants::MISMATCH,
       ":calculated_checksum" => calculated_checksum,
       ":timestamp" => Time.now.getutc.iso8601(3)
      },
      update_expression: "SET #{FixityConstants::FIXITY_STATUS} = :fixity_status, "\
                             "#{FixityConstants::FIXITY_OUTCOME} = :fixity_outcome, " \
                             "#{FixityConstants::CALCULATED_CHECKSUM} = :calculated_checksum, " \
                             "#{FixityConstants::LAST_UPDATED} = :timestamp, " \
                             "#{FixityConstants::MISMATCH} = :mismatch"
    })
  end

  def self.update_fixity_error(s3_key)
    FixityConstants::DYNAMODB_CLIENT.update_item({
      table_name: FixityConstants::FIXITY_TABLE_NAME,
      key: {
        FixityConstants::S3_KEY => s3_key
      },
      expression_attribute_names: {
        "#E" => FixityConstants::ERROR,
      },
      expression_attribute_values: {
        ":error" => FixityConstants::TRUE,
        ":fixity_status" => FixityConstants::ERROR,
        ":timestamp" => Time.now.getutc.iso8601(3)
      },
      update_expression: "SET #{FixityConstants::FIXITY_STATUS} = :fixity_status, "\
                             "#{FixityConstants::LAST_UPDATED} = :timestamp, " \
                             "#E = :error"
    })
  end
end

