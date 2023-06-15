# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'
require 'aws-sdk-s3control'
require 'pg'
require 'cgi'

require_relative 'fixity/fixity_constants'
require_relative 'fixity/fixity_secrets'
require_relative 'fixity/batch_item'
require_relative 'send_message'

class RestoreFiles
  MAX_BATCH_COUNT = 1000
  MAX_BATCH_SIZE = 16*1024**2*MAX_BATCH_COUNT

  def self.get_batch
    #get information from medusa DB for a batch of files to be restored(File ID, S3 key, initial checksum)
    # medusa_item => make object with file_id, s3_key, initial_checksum
    batch_size = 0
    batch = []

    time_start = Time.now
    id = get_medusa_id
    return nil if id.nil?

    max_id = get_max_id
    return nil if max_id.nil?

    evaluate_done(id, max_id) #TODO return if true?

    #query medusa and add files to batch
    batch_count = 0
    # while batch_size < MAX_BATCH_SIZE && batch_count < MAX_BATCH_COUNT && id <= max_id
    while batch_count < MAX_BATCH_COUNT && id <= max_id
      begin
        file_row = get_file(id)

        if file_row.nil?
          id = id+1
          next
        end

        directory_id = file_row["cfs_directory_id"]
        name = file_row["name"]
        size = file_row["size"].to_i
        initial_checksum = file_row["md5_sum"]

        # break if (size + batch_size > MAX_BATCH_SIZE)

        s3_key = get_path(directory_id, name)
        batch_item = BatchItem.new(s3_key, id, initial_checksum)
        batch.push(batch_item)

        #TODO move to ensure to continue after error?
        id = id+1
        evaluate_done(id, max_id) #TODO break if done?

        batch_size = batch_size + size
        batch_count = batch_count + 1

      rescue StandardError => e
        error_message = "Error getting file information for file #{id} from medusa db: #{e.backtrace}"
        FixityConstants::LOGGER.error(error_message)
        break #increment id and continue?
      end
    end
    put_medusa_id(id)

    time_end = Time.now
    duration = time_end - time_start

    FixityConstants::LOGGER.info("Get batch duration to process #{batch_count} files: #{duration}")
    restore_batch(batch) #call one by one or together?
  end

  def self.get_medusa_id
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
      return nil
    end

    return nil if query_resp.nil?
    query_resp.items[0][FixityConstants::FILE_ID].to_i
  end

  def self.get_max_id
    max_resp = FixitySecrets::MEDUSA_DB.exec("SELECT MAX(id) FROM cfs_files")
    max_resp.first["max"].to_i
  end

  def self.evaluate_done(id, max_id)
    done = id >= max_id
    done_message = "DONE: fixity id matches maximum file id in medusa"
    FixityConstants::LOGGER.error(done_message) if done
    done
  end

  def self.get_file(id)
    #TODO optimize to get multiple files per call to medusa DB
    file_result = FixitySecrets::MEDUSA_DB.exec( "SELECT * FROM cfs_files WHERE id=#{id.to_s}" )
    file_result.first
  end

  def self.get_path(directory_id, path)
    while directory_id
      dir_result = FixitySecrets::MEDUSA_DB.exec( "SELECT * FROM cfs_directories WHERE id=#{directory_id}" )
      dir_row = dir_result.first
      dir_path = dir_row["path"]
      path.prepend(dir_path,'/')
      directory_id = dir_row["parent_id"]
      parent_type = dir_row["parent_type"]
      break if parent_type != "CfsDirectory"
    end
    CGI.escape(path).gsub('%2F', '/')
  end

  def self.put_medusa_id(id)
    FixityConstants::DYNAMODB_CLIENT.put_item({
      table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
      item: {
        FixityConstants::ID_TYPE => FixityConstants::CURRENT_ID,
        FixityConstants::FILE_ID => id.to_s,
      }
    })
  end

  def self.get_batch_from_list(list)
    batch = []
    list.each do |id|
      file_row = get_file(id)
      if file_row.nil?
        FixityConstants::LOGGER.error("File with id #{id} not found in medusa DB")
        next
      end

      directory_id = file_row["cfs_directory_id"]
      name = file_row["name"]

      initial_checksum = file_row["md5_sum"]
      s3_key = get_path(directory_id, name)
      batch_item = BatchItem.new(s3_key, id, initial_checksum)
      batch.push(batch_item)
    end
    restore_batch(batch)
  end

  def self.restore_batch(batch)
    # make restore requests to S3 glacier for next batch of files to be processed
    # files may take up to 48 hours to restore and are only available for 24 to save costs
    time_start = Time.now
    #TODO check if batch is empty
    batch.each do |fixity_item|
      begin
        message = "RESTORING: File Id= #{fixity_item.file_id}, S3 Key: #{fixity_item.s3_key}"
        FixityConstants::LOGGER.info(message)
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
        put_missing_key(fixity_item)
        #SendMessage.send_message(file_id, nil, FixityConstants::FALSE, FixityConstants::FAILURE, error_message)
        next

      rescue StandardError => e
        # Error requesting object restoration, add to dynamodb table for retry?
        # Send error message to medusa
        #TODO add to Dynamodb for retry
        error_message = "Error restoring object #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.message}"
        FixityConstants::LOGGER.error(error_message)

      end
      put_batch_item(fixity_item)
    end
    time_end = Time.now
    duration = time_end - time_start
    FixityConstants::LOGGER.info("Restore batch duration to process #{batch.length()} files: #{duration}")
  end

  def self.put_batch_item(fixity_item)
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
      FixityConstants::LOGGER.error(error_message)
    end
  end

  def self.put_missing_key(fixity_item)
    begin
      FixityConstants::DYNAMODB_CLIENT.put_item({
        table_name: FixityConstants::MISSING_KEYS_TABLE_NAME,
        item: {
          FixityConstants::S3_KEY => fixity_item.s3_key,
          FixityConstants::FILE_ID => fixity_item.file_id,
          FixityConstants::INITIAL_CHECKSUM => fixity_item.initial_checksum,
          FixityConstants::LAST_UPDATED => Time.now.getutc.iso8601(3)
        }
      })
    rescue StandardError => e
      error_message = "Error putting item in dynamodb table for #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
  end
end
