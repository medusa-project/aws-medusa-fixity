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
require_relative 'medusa_sqs'

class ProcessBatchReports
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :s3, :s3_control, :dynamodb, :medusa_sqs

  def initialize(s3 = S3.new, dynamodb = Dynamodb.new, s3_control = S3Control.new, medusa_sqs = MedusaSqs.new)
    @s3 = s3
    @dynamodb = dynamodb
    @s3_control = s3_control
    @medusa_sqs = medusa_sqs
  end

  def process_failures
    # TODO: add test
    job_id = get_job_id
    return nil if job_id.nil?

    job_info = get_job_info(job_id)
    return nil if job_info.nil?

    job_status = get_job_status(job_info)

    if job_status == Settings.aws.s3.failed
      failed_message = "Batch job: #{job_id} failed"
      FixityConstants::LOGGER.error(failed_message)
      remove_job_id(job_id)
      return
    end

    return if job_status != Settings.aws.s3.complete

    get_duration(job_info)
    job_failures = get_tasks_failed(job_info)

    if job_failures.zero?
      remove_job_id(job_id)
      return nil
    end

    manifest_key = get_manifest_key(job_id)
    return nil if manifest_key.nil?

    error_batch = parse_completion_report(manifest_key)
    return nil if error_batch.empty?

    put_requests = @dynamodb.get_put_requests(error_batch)
    @dynamodb.batch_write_items(Settings.aws.dynamodb.restoration_errors_table_name, put_requests)

    remove_job_id(job_id)
  end

  def get_job_id
    table_name = Settings.aws.dynamodb.batch_job_ids_table_name
    scan_resp = @dynamodb.scan(table_name, 1)
    return nil if scan_resp.nil? || scan_resp.items.empty?

    scan_resp.items[0][Settings.aws.dynamodb.job_id]
  end

  def get_job_info(job_id)
    describe_resp = @s3_control.describe_job(job_id)
    return nil if describe_resp.nil?

    describe_resp
  end

  def get_duration(job_info)
    job_duration = job_info.job.progress_summary.timers.elapsed_time_in_active_seconds
    job_tasks = job_info.job.progress_summary.total_number_of_tasks
    job_id = job_info.job.job_id
    FixityConstants::LOGGER.info("Batch restoration job #{job_id} duration to process #{job_tasks} files: #{job_duration} seconds")
  end

  def get_tasks_failed(job_info)
    job_info.job.progress_summary.number_of_tasks_failed
  end

  def get_job_status(job_info)
    job_info.job.status
  end

  def get_manifest_key(job_id)
    key = "#{Settings.aws.s3.batch_prefix}/job-#{job_id}/manifest.json"
    s3_json_resp = @s3.get_object(Settings.aws.s3.fixity_bucket, key)
    return nil if s3_json_resp.nil?

    JSON.parse(s3_json_resp.body.read)['Results'][0]['Key']
  end

  def parse_completion_report(manifest_key)
    response_target = './report.csv'
    @s3.get_object_to_response_target(Settings.aws.s3.fixity_bucket, manifest_key, response_target)
    batch_completion_table = CSV.new(File.read('report.csv'))
    error_batch = []
    batch_completion_table.each do |row|
      bucket, key, _version_id, _task_status, error_code, https_status_code, result_message = row
      file_id = get_file_id(key)
      error_message = "Object: #{file_id} with key: #{key} failed during restoration job with error #{https_status_code}:#{result_message}"
      FixityConstants::LOGGER.error(error_message)

      # re-request restoration for files with "AccessDenied" https_status_codes, this could indicate the file is missing
      file_found = @s3.found?(Settings.aws.s3.backup_bucket, key)
      error_message = "Object with key: #{key} not found in bucket: #{bucket}"
      if file_found
        @s3.restore_object(@dynamodb, Settings.aws.s3.backup_bucket, key, file_id)
      else
        @medusa_sqs.send_medusa_message(file_id, nil, false, Settings.aws.sqs.success, error_message)
        https_status_code = Settings.aws.dynamodb.not_found
      end

      error_hash = {
        Settings.aws.dynamodb.s3_key => key,
        Settings.aws.dynamodb.file_id => file_id,
        Settings.aws.dynamodb.err_code => error_code,
        Settings.aws.dynamodb.https_status_code => https_status_code,
        Settings.aws.dynamodb.last_updated => Time.now.getutc.iso8601(3)
      }
      error_batch.push(error_hash)
    end
    error_batch
  end

  def get_file_id(s3_key)
    table_name = Settings.aws.dynamodb.fixity_table_name
    limit = 1
    expr_attr_vals = { ':s3_key' => s3_key }
    key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :s3_key"
    query_resp = @dynamodb.query(table_name, limit, expr_attr_vals, key_cond_expr)
    return nil if query_resp.nil? || query_resp.items.empty?

    query_resp.items[0][Settings.aws.dynamodb.file_id]
  end

  def remove_job_id(job_id)
    key = { Settings.aws.dynamodb.job_id => job_id }
    table_name = Settings.aws.dynamodb.batch_job_ids_table_name
    @dynamodb.delete_item(key, table_name)
  end
end
