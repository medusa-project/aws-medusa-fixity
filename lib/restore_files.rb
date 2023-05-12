# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'
require 'pg'

require_relative 'fixity/fixity_constants'
require_relative 'fixity/fixity_secrets'
require_relative 'fixity/medusa_item'
require_relative 'send_message'

class RestoreFiles
  MAX_BATCH_SIZE = 189797120
  def self.get_batch
    #get information from medusa DB for a batch of files to be restored(File ID, S3 key, initial checksum)
    # medusa_item => make object with file_id, s3_key, initial_checksum
    batch_size = 0
    batch = []
    begin
      #Get medusa id to start next batch from dynamodb
      query_resp= FixityConstants::DYNAMODB_CLIENT.query({
        table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
        limit: 1,
        scan_index_forward: true,
        expression_attribute_values: {
          ":file_type" => FixityConstants::CURRENT_ID,
        },
        key_condition_expression: "#{FixityConstants::ID_TYPE} = :file_type",
      })
    rescue StandardError => e
      # Error getting current medusa id
      # Send error message to medusa
      error_message = "Error getting medusa id to query the database: #{e.message}"
      FixityConstants::LOGGER.error(error_message)

    end

    return nil if query_resp.nil?
    max_resp = FixitySecrets::MEDUSA_DB.exec("SELECT MAX(id) FROM cfs_files")
    max_id = max_resp.first["max"].to_i

    id = query_resp.items[0][FixityConstants::FILE_ID].to_i

    #query medusa and add files to batch
    while batch_size < MAX_BATCH_SIZE && id <= max_id
      begin
        file_result = FixitySecrets::MEDUSA_DB.exec( "SELECT * FROM cfs_files WHERE id=#{id.to_s}" )
        file_row = file_result.first

        if file_row.nil?
          id = id+1
          next
        end

        directory_id = file_row["cfs_directory_id"]
        name = file_row["name"]
        size = file_row["size"].to_i

        break if (size + batch_size > MAX_BATCH_SIZE)

        checksum = file_row["md5_sum"]
        path = name
        while directory_id
          dir_result = FixitySecrets::MEDUSA_DB.exec( "SELECT * FROM cfs_directories WHERE id=#{directory_id}" )
          dir_row = dir_result.first
          dir_path = dir_row["path"]
          path.prepend(dir_path,'/')
          directory_id = dir_row["parent_id"]
          parent_type = dir_row["parent_type"]
          break if parent_type != "CfsDirectory"
        end
        s3_key = path
        file_id = id
        initial_checksum = checksum
        medusa_item = MedusaItem.new(s3_key, file_id, initial_checksum)
        batch.push(medusa_item)
        id = id+1
        batch_size = batch_size + size
      rescue StandardError => e
        error_message = "Error getting file information for file #{id} from medusa db: #{e.backtrace}"
        FixityConstants::LOGGER.error(error_message)
        break
      end
    end

    FixityConstants::DYNAMODB_CLIENT.put_item({
      table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
      item: {
        FixityConstants::ID_TYPE => FixityConstants::CURRENT_ID,
        FixityConstants::FILE_ID => id.to_s,
      }
    })

    batch.each do |fixity_item|
      FixityConstants::LOGGER.info(fixity_item.s3_key)
      FixityConstants::LOGGER.info(fixity_item.file_id)
      FixityConstants::LOGGER.info(fixity_item.initial_checksum)
    end
    restore_batch(batch) #call one by one or together?
  end

  def self.restore_batch(batch)
    # make restore requests to S3 glacier for next batch of files to be processed
    # files may take up to 48 hours to restore and are only available for 24 to save costs
    batch.each do |fixity_item|
      begin
        FixityConstants::S3_CLIENT.restore_object({
          bucket: FixityConstants::BACKUP_BUCKET,
          key: fixity_item.s3_key,
          restore_request: {
            days: 1,
            glacier_job_parameters: {
              tier: FixityConstants::BULK
            },
          },
        })
      rescue Aws::S3::Errors::NoSuchKey => e
        #File not found in S3 bucket, don't add to dynamodb table (maybe add to separate table for investigation?)
        error_message = "Error getting object #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.backtrace}"
        FixityConstants::LOGGER.error(error_message)
        #SendMessage.send_message(file_id, nil, FixityConstants::FALSE, FixityConstants::FAILURE, error_message)

      rescue StandardError => e
        # Error requesting object restoration, add to dynamodb table for retry?
        # Send error message to medusa
        error_message = "Error getting object #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.message}"
        FixityConstants::LOGGER.error(error_message)

      end
      begin
        FixityConstants::DYNAMODB_CLIENT.put_item({
          table_name: FixityConstants::FIXITY_TABLE_NAME,
          item: {
            FixityConstants::S3_KEY => fixity_item.s3_key,
            FixityConstants::FILE_ID => fixity_item.file_id,
            FixityConstants::INITIAL_CHECKSUM => fixity_item.initial_checksum,
            FixityConstants::RESTORATION_STATUS => FixityConstants::REQUESTED,
            FixityConstants::LAST_UPDATED => Time.now.getutc.iso8601(3)
          }
        })
      rescue StandardError => e
        error_message = "Error putting item in dynamodb table for #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.message}"
        FixityConstants::LOGGER.info(error_message)
      end
    end
  end
end
