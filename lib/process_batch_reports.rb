# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'
require 'aws-sdk-s3control'
require 'csv'

require_relative 'fixity/dynamodb'
require_relative 'fixity/fixity_constants'

class ProcessBatchReports

  def self.process_failures
    #TODO move job ids to separate dynamodb table
    job_id = get_job_id

    job_failures = get_tasks_failed(job_id)
    return nil if job_failures.zero?

    manifest_key = get_manifest_key(job_id)
    error_batch = parse_completion_report(manifest_key)
    return nil if error_batch.empty?

    Dynamodb.put_batch_items_in_table(FixityConstants::RESTORATION_ERRORS_TABLE_NAME, error_batch)
    
    remove_job_id
  end

  def self.get_job_id
    query_resp= FixityConstants::DYNAMODB_CLIENT.query({
      table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
      limit: 1,
      scan_index_forward: true,
      expression_attribute_values: {
        ":job_id" => FixityConstants::JOB_ID,
      },
      key_condition_expression: "#{FixityConstants::ID_TYPE} = :job_id",
    })
    puts query_resp
  end

  #TODO refactor to separate duration from failures, add in check to see if job is complete
  def self.get_tasks_failed(job_id)
    describe_resp = FixityConstants::S3_CONTROL_CLIENT.describe_job(account_id: FixityConstants::ACCOUNT_ID, job_id: job_id)
    job_status = describe_resp.job.status
    job_duration = describe_resp.job.progress_summary.timers.elapsed_time_in_active_seconds
    job_tasks = describe_resp.job.progress_summary.total_number_of_tasks
    FixityConstants::LOGGER.info("Batch restoration job duration to process #{job_tasks} files: #{job_duration}")
    return describe_resp.job.progress_summary.number_of_tasks_failed
  end

  def self.get_manifest_key(job_id)
    s3_json_resp = S3_CLIENT.get_object({
      bucket: FixityConstants::BACKUP_BUCKET, # required
      key: "#{FixityConstants::BATCH_PREFIX}/job-#{job_id}/manifest.json", # required
    })
    key = JSON.parse(s3_json_resp.body.read)["Results"][0]["Key"]

    return key
  end

  def self.parse_completion_report(manifest_key)

    FixityConstants::S3_CLIENT.get_object({
                       bucket: FixityConstants::BACKUP_BUCKET, # required
                       key: manifest_key, # required
                       response_target: './report.csv',
                     })
    batch_completion_table = CSV.new(File.read("report.csv"))
    error_batch = []
    batch_completion_table.each do |row|
      bucket, key, version_id, task_status, error_code, https_status_code, result_message = row
      file_id = get_file_id(key)
      error_message = "Object: #{file_id} with key: #{key} failed during restoration job with error #{error_code}:#{https_status_code}"
      FixityConstants::LOGGER.error(error_message)
      error_hash = {
        FixityConstants::S3_KEY => key,
        FixityConstants::FILE_ID => file_id,
        FixityConstants::ERR_CODE => error_code,
        FixityConstants::HTTPS_STATUS_CODE => https_status_code,
        FixityConstants::LAST_UPDATED => Time.now.getutc.iso8601(3)
      }
      error_batch.push(error_hash)
    end
    return error_batch
  end

  #TODO implement get file_id
  def self.get_file_id(s3_key)
    begin
      #Get medusa id to start next batch from dynamodb
      query_resp= FixityConstants::DYNAMODB_CLIENT.query({
         table_name: FixityConstants::FIXITY_TABLE_NAME,
         limit: 1,
         scan_index_forward: true,
         expression_attribute_values: {
           ":s3_key" => s3_key,
         },
         key_condition_expression: "#{FixityConstants::S3_KEY} = :s3_key",
       })
    rescue StandardError => e
      # Error getting current request token
      error_message = "Error getting file id for key #{s3_key}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      return nil
    end
    query_resp[0]["FileId"]
  end

  def self.remove_job_id
    resp = FixityConstants::DYNAMODB_CLIENT.delete_item({
      key: {
        FixityConstants::ID_TYPE => FixityConstants::JOB_ID,
      },
      table_name: FixityConstants::MEDUSA_DB_ID_TABLE_NAME,
    })
  end
end
