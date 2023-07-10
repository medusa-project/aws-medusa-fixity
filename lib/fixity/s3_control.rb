# frozen_string_literal: true
require 'aws-sdk-s3control'
require 'config'
require_relative 'fixity_constants'

class S3Control
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :s3_control_client

  def initialize(s3_control_client = FixityConstants::S3_CONTROL_CLIENT)
    @s3_control_client = s3_control_client
  end

  def describe_job(job_id)
    begin
      resp = @s3_control_client.describe_job({account_id: Settings.aws.account_id, job_id: job_id})
    rescue StandardError => e
      error_message = "Error describing job: #{job_id}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    return resp
  end

  def create_job(manifest, token, etag)
    begin
      resp = @s3_control_client.create_job({
        account_id: Settings.aws.account_id,
        confirmation_required: false,
        operation: {
          s3_initiate_restore_object: {
            expiration_in_days: 1,
            glacier_job_tier: "BULK", # accepts BULK, STANDARD
          }
        },
        report: {
          bucket: Settings.aws.s3.backup_bucket_arn,
          format: "Report_CSV_20180820", # accepts Report_CSV_20180820
          enabled: true, # required
          prefix: Settings.aws.s3.batch_prefix,
          report_scope: "FailedTasksOnly", # accepts AllTasks, FailedTasksOnly
        },
        client_request_token: "#{token}", # required
        manifest: {
          spec: { # required
                  format: "S3BatchOperations_CSV_20180820", # required, accepts S3BatchOperations_CSV_20180820, S3InventoryReport_CSV_20161130
                  fields: %w[Bucket Key], # accepts Ignore, Bucket, Key, VersionId
          },
          location: { # required
                      object_arn: "#{Settings.aws.s3.backup_bucket_arn}/fixity/#{manifest}", # required
                      etag: etag, # required
          },
        },
        priority: 10,
        role_arn: Settings.aws.s3.batch_arn, # required
      })
    rescue StandardError => e
      error_message = "Error creating job with manifest: #{manifest}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    return resp
  end



end
