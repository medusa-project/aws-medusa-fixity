require 'minitest/autorun'
require 'aws-sdk-s3control'
require 'config'

require_relative '../lib/fixity/s3_control'

class TestS3Control < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", 'test'))

  def setup
    @mock_s3_control_client = Minitest::Mock.new
    @s3_control = S3Control.new(@mock_s3_control_client)
  end

  def teardown
    File.truncate('logs/fixity.log', 0)
  end

  def test_describe_job
    account_id = Settings.aws.account_id
    job_id = 'job-1234567890'
    args_verification = [account_id: account_id, job_id: job_id]
    @mock_s3_control_client.expect(:describe_job, [], args_verification)
    @s3_control.describe_job(job_id)
    assert_mock(@mock_s3_control_client)
  end

  def test_create_job
    account_id = Settings.aws.account_id
    manifest = "manifest-#{Time.new(1).strftime('%F-%H:%M')}.csv"
    token = 123
    etag = '12345678901234567890123456789012'
    operation = { s3_initiate_restore_object: { expiration_in_days: 1, glacier_job_tier: 'BULK' } }
    report = { bucket: Settings.aws.s3.fixity_bucket_arn, format: 'Report_CSV_20180820', enabled: true, prefix: Settings.aws.s3.batch_prefix, report_scope: 'FailedTasksOnly' }
    args = {
      account_id: account_id,
      confirmation_required: false,
      operation: operation,
      report: report,
      client_request_token: token.to_s, # required
      manifest: { spec: { format: 'S3BatchOperations_CSV_20180820', fields: %w[Bucket Key] },
                  location: { object_arn: "#{Settings.aws.s3.fixity_bucket}/fixity/#{manifest}", etag: etag } },
      priority: 10,
      role_arn: Settings.aws.s3.batch_arn # required
    }
    args_verification = [args]
    @mock_s3_control_client.expect(:create_job, [], args_verification)
    @s3_control.create_job(manifest, token, etag)
    assert_mock(@mock_s3_control_client)
  end
end
