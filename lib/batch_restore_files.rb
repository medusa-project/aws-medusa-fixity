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
class BatchRestoreFiles
  MAX_BATCH_COUNT = 1000
  MAX_BATCH_SIZE = 16*1024**2*MAX_BATCH_COUNT

  def self.get_batch
    #get information from medusa DB for a batch of files to be restored(File ID, S3 key, initial checksum)
    # medusa_item => make object with file_id, s3_key, initial_checksum
    batch_size = 0

    time_start = Time.now
    id = get_medusa_id
    return nil if id.nil?

    max_id = get_max_id
    return nil if max_id.nil?

    evaluate_done(id, max_id) #TODO return if true?

    #query medusa and add files to batch
    batch_count = 0
    manifest = "manifest-#{Time.now.strftime('%F-%H:%M')}.csv"
    # while batch_size < MAX_BATCH_SIZE && batch_count < MAX_BATCH_COUNT && id <= max_id
    while batch_count < MAX_BATCH_COUNT && id <= max_id
      begin
        #TODO optimize to get multiple files per call to medusa DB
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
        put_batch_item(batch_item)

        open(manifest, 'a') { |f|
          f.puts "#{FixityConstants::BACKUP_BUCKET},#{s3_key}"
        }

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
    etag = put_manifest(manifest)
    FixityConstants::LOGGER.info("Manifest etag: #{etag}")
    #send_batch_job(manifest, etag)
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

  def self.get_files_in_batches(id)
    #TODO add batch size as class variable to keep track of file sizes
    medusa_files = []
    id_iterator = id + 10
    file_result = FixitySecrets::MEDUSA_DB.exec( "SELECT * FROM cfs_files WHERE id>=#{id.to_s} AND  id<=#{id_iterator}")
    file_result.each do |file_row|
      next if file_row.nil?
      file_id = file_row["id"]
      directory_id = file_row["cfs_directory_id"]
      name = file_row["name"]
      size = file_row["size"].to_i
      initial_checksum = file_row["md5_sum"]

      medusa_files.push(MedusaFile.new(name, file_id, directory_id, initial_checksum))
    end
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

  def self.put_batch_item(batch_item)
    begin
      FixityConstants::DYNAMODB_CLIENT.put_item({
        table_name: FixityConstants::FIXITY_TABLE_NAME,
        item: {
          FixityConstants::S3_KEY => batch_item.s3_key,
          FixityConstants::FILE_ID => batch_item.file_id,
          FixityConstants::INITIAL_CHECKSUM => batch_item.initial_checksum,
          FixityConstants::RESTORATION_STATUS => FixityConstants::REQUESTED,
          FixityConstants::LAST_UPDATED => Time.now.getutc.iso8601(3)
        }
      })
    rescue StandardError => e
      error_message = "Error putting item in dynamodb table for #{batch_item.s3_key} with ID #{batch_item.file_id}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
  end

  def self.put_manifest(manifest)
    s3_resp = FixityConstants::S3_CLIENT.put_object(
      body: File.new(manifest),
      bucket: "#{FixityConstants::BACKUP_BUCKET}",
      key: "fixity/#{manifest}",
    )
    s3_resp.etag
  end

  def self.get_batch_from_list(list)
    manifest = "manifest-#{Time.now.strftime('%F-%H:%M')}.csv"
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
      put_batch_item(batch_item)

      open(manifest, 'a') { |f|
        f.puts "#{FixityConstants::BACKUP_BUCKET},#{s3_key}"
      }
    end
    etag = put_manifest(manifest)
    send_batch_job(manifest, etag)
  end

  def self.send_batch_job(manifest, etag)
    token = get_request_token + 1
    begin
      resp = FixityConstants::S3_CONTROL_CLIENT.create_job({
        account_id: FixityConstants::ACCOUNT_ID,
        confirmation_required: false,
        operation: {
          s3_initiate_restore_object: {
            expiration_in_days: 1,
            glacier_job_tier: "BULK", # accepts BULK, STANDARD
          }
        },
        report: {
          bucket: FixityConstants::BACKUP_BUCKET_ARN,
          format: "Report_CSV_20180820", # accepts Report_CSV_20180820
          enabled: true, # required
          prefix: FixityConstants::BATCH_PREFIX,
          report_scope: "FailedTasksOnly", # accepts AllTasks, FailedTasksOnly
        },
        client_request_token: "#{token}", # required
        manifest: {
          spec: { # required
                  format: "S3BatchOperations_CSV_20180820", # required, accepts S3BatchOperations_CSV_20180820, S3InventoryReport_CSV_20161130
                  fields: ["Bucket","Key"], # accepts Ignore, Bucket, Key, VersionId
          },
          location: { # required
                  object_arn: "#{FixityConstants::BACKUP_BUCKET_ARN}/fixity/#{manifest}", # required
                  etag: etag, # required
          },
        },
        priority: 10,
        role_arn: FixityConstants::BATCH_ROLE_ARN, # required
      })
      job_id = resp.job_id
      put_job_id(job_id)
      batch_job_message = "Batch restore job sent with id #{job_id}"
      FixityConstants::LOGGER.info(batch_job_message)
    rescue StandardError => e
      error_message = "Error sending batch job: #{e.full_message}"
      FixityConstants::LOGGER.error(error_message)
    end
    FixityConstants::LOGGER.info(resp)
    put_request_token(token)
  end

  def self.get_request_token
    begin
      #Get medusa id to start next batch from dynamodb
      query_resp= FixityConstants::DYNAMODB_CLIENT.query({
        table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
        limit: 1,
        scan_index_forward: true,
        expression_attribute_values: {
          ":request_token" => FixityConstants::CURRENT_REQUEST_TOKEN,
        },
        key_condition_expression: "#{FixityConstants::ID_TYPE} = :request_token",
      })
    rescue StandardError => e
      # Error getting current request token
      error_message = "Error getting request token to send batch job: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      return nil
    end

    return nil if query_resp.nil?
    query_resp.items[0][FixityConstants::FILE_ID].to_i
  end

  def self.put_request_token(token)
    FixityConstants::DYNAMODB_CLIENT.put_item({
      table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
      item: {
        FixityConstants::ID_TYPE => FixityConstants::CURRENT_REQUEST_TOKEN,
        FixityConstants::FILE_ID => token.to_s,
      }
    })
  end

  def self.put_job_id(job_id)
    FixityConstants::DYNAMODB_CLIENT.put_item({
      table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
      item: {
        FixityConstants::ID_TYPE => FixityConstants::JOB_ID,
        FixityConstants::FILE_ID => job_id,
        FixityConstants::PROCESSED => FixityConstants::FALSE
      }
    })
  end
end
