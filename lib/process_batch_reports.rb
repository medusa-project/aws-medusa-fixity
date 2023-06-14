# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'
require 'aws-sdk-s3control'
require 'csv'

require_relative 'fixity/fixity_constants'

class ProcessBatchReports

  def self.process_failures
    job_id = get_job_id
    job_failures = get_tasks_failed(job_id)
    return nil if job_failures.zero?

    manifest_key = get_manifest_key(job_id)



  end

  #TODO get job id from dynamodb table
  def self.get_job_id
    #return job id from dyanamodb
  end

  #TODO refeactor to separate duration from failures
  def self.get_tasks_failed(job_id)
    describe_resp = FixityConstants::S3_CONTROL_CLIENT.describe_job(account_id: FixityConstants::ACCOUNT_ID, job_id: job_id)
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
    batch_completion_table.each do |row|
      bucket, key, version_id, task_status, error_code, https_status_code, result_message = row
      error_message = "Object #{key} failed during restoration job with error #{error_code}:#{https_status_code}"
      FixityConstants::LOGGER.error(error_message)
      #TODO handle errors with dynamodb
    end
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
    query_resp[0]
  end

  def self.put_error(key, error)

    FixityConstants::DYNAMODB_CLIENT.put_item({
      table_name: FixityConstants::MISSING_KEYS_TABLE_NAME,
      item: {
        FixityConstants::ID_TYPE => FixityConstants::CURRENT_REQUEST_TOKEN,
        FixityConstants::FILE_ID => token.to_s,
      }
    })
  end

end
