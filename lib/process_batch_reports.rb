# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'
require 'aws-sdk-s3control'
require 'csv'
require 'config'

require_relative 'fixity/dynamodb'
require_relative 'fixity/s3'
require_relative 'fixity/s3_control'
require_relative 'fixity/fixity_constants'

class ProcessBatchReports
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  def self.process_failures
    dynamodb = Dynamodb.new
    s3 = S3.new
    s3_control = S3Control.new
    job_id = get_job_id(dynamodb)
    job_info = get_job_info(s3_control, job_id)
    return nil if job_info.nil?

    return if get_job_status(job_info) != Settings.aws.s3.complete
    get_duration(job_info)
    job_failures = get_tasks_failed(job_info)

    if job_failures.zero?
      remove_job_id(dynamodb, job_id)
      return nil
    end

    manifest_key = get_manifest_key(s3, job_id)
    return nil if manifest_key.nil?
    error_batch = parse_completion_report(dynamodb, s3, manifest_key)
    return nil if error_batch.empty?

    put_requests = dynamodb.get_put_requests(error_batch)
    dynamodb.batch_write_items(Settings.aws.dynamodb.restoration_errors_table_name, put_requests)

    remove_job_id(dynamodb, job_id)
  end

  def self.get_job_id(dynamodb)
    table_name = Settings.aws.dynamodb.batch_job_ids_table_name
    scan_resp = dynamodb.scan(table_name, 1)
    return nil if scan_resp.nil?
    return scan_resp.items[0][Settings.aws.dynamodb.job_id]
  end

  def self.get_job_info(s3_control, job_id)
    describe_resp = s3_control.describe_job(job_id)
    return nil if describe_resp.nil?
    describe_resp
  end

  def self.get_duration(job_info)
    job_duration = job_info.job.progress_summary.timers.elapsed_time_in_active_seconds
    job_tasks = job_info.job.progress_summary.total_number_of_tasks
    job_id = job_info.job.job_id
    FixityConstants::LOGGER.info("Batch restoration job #{job_id} duration to process #{job_tasks} files: #{job_duration} seconds")
  end

  def self.get_tasks_failed(job_info)
    job_info.job.progress_summary.number_of_tasks_failed
  end

  def self.get_job_status(job_info)
    job_info.job.status
  end

  def self.get_manifest_key(s3, job_id)
    key = "#{Settings.aws.s3.batch_prefix}/job-#{job_id}/manifest.json"
    s3_json_resp = s3.get_object(Settings.aws.s3.backup_bucket, key)
    return nil if s3_json_resp.nil?
    manifest_key = JSON.parse(s3_json_resp.body.read)["Results"][0]["Key"]
    return manifest_key
  end

  def self.parse_completion_report(dynamodb, s3, manifest_key)
    response_target = "./report.csv"
    s3.get_object_to_response_target(Settings.aws.s3.backup_bucket, manifest_key, response_target)
    batch_completion_table = CSV.new(File.read("report.csv"))
    error_batch = []
    #TODO test key not in s3 bucket
    batch_completion_table.each do |row|
      bucket, key, version_id, task_status, error_code, https_status_code, result_message = row
      file_id = get_file_id(dynamodb, key)
      error_message = "Object: #{file_id} with key: #{key} failed during restoration job with error #{error_code}:#{https_status_code}"
      FixityConstants::LOGGER.error(error_message)
      error_hash = {
        Settings.aws.dynamodb.s3_key => key,
        Settings.aws.dynamodb.file_id => file_id,
        Settings.aws.dynamodb.err_code => error_code,
        Settings.aws.dynamodb.https_status_code => https_status_code,
        Settings.aws.dynamodb.last_updated => Time.now.getutc.iso8601(3)
      }
      error_batch.push(error_hash)
    end
    return error_batch
  end

  def self.get_file_id(dynamodb, s3_key)
    table_name = Settings.aws.dynamodb.fixity_table_name
    limit = 1
    expr_attr_vals = { ":s3_key" => s3_key,}
    key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :s3_key"
    query_resp= dynamodb.query(table_name, limit, expr_attr_vals, key_cond_expr)
    return nil if query_resp.nil?
    query_resp.items[0][Settings.aws.dynamodb.file_id]
  end

  def self.remove_job_id(dynamodb, job_id)
    key = { Settings.aws.dynamodb.job_id => job_id,}
    table_name = Settings.aws.dynamodb.batch_job_ids_table_name
    dynamodb.delete_item(key, table_name)
  end
end
