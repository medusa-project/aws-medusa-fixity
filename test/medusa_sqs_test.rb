require 'minitest/autorun'
require 'config'

require_relative '../lib/medusa_sqs'

class TestMedusaSqs < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))

  def setup
    @mock_medusa_sqs_client = Minitest::Mock.new
    @medusa_sqs = MedusaSqs.new(@mock_medusa_sqs_client)
  end
  def test_send_medusa_message
    file_id = "123"
    checksum = "12345678901234567890123456789012"
    found = true
    status = Settings.aws.sqs.success
    checksums = {Settings.aws.sqs.md5 => checksum}
    parameters = {Settings.aws.sqs.checksums => checksums, Settings.aws.sqs.found => found}
    passthrough = {Settings.aws.sqs.cfs_file_id => file_id, Settings.aws.sqs.cfs_file_class => Settings.aws.sqs.cfs_file}
    message_exp = {Settings.aws.sqs.action => Settings.aws.sqs.file_fixity,
                   Settings.aws.sqs.status => status,
                   Settings.aws.sqs.parameters => parameters,
                   Settings.aws.sqs.passthrough => passthrough}
    args_verification = [queue_url: Settings.aws.sqs.medusa_queue_url, message_body: message_exp.to_json, message_attributes: {}]
    @mock_medusa_sqs_client.expect(:send_message, [], args_verification)
    @medusa_sqs.send_medusa_message(file_id, checksum, found, status)
    assert_mock(@mock_medusa_sqs_client)
  end

  def test_send_medusa_message_error_message
    file_id = "123"
    checksum = "12345678901234567890123456789012"
    found = false
    status = Settings.aws.sqs.failure
    error_message = "Test error message"
    checksums = {Settings.aws.sqs.md5 => checksum}
    parameters = {Settings.aws.sqs.checksums => checksums, Settings.aws.sqs.found => found}
    passthrough = {Settings.aws.sqs.cfs_file_id => file_id, Settings.aws.sqs.cfs_file_class => Settings.aws.sqs.cfs_file}
    message_exp = {Settings.aws.sqs.action => Settings.aws.sqs.file_fixity,
                   Settings.aws.sqs.status => status,
                   Settings.aws.sqs.error_message => error_message,
                   Settings.aws.sqs.parameters => parameters,
                   Settings.aws.sqs.passthrough => passthrough}
    args_verification = [queue_url: Settings.aws.sqs.medusa_queue_url, message_body: message_exp.to_json, message_attributes: {}]
    @mock_medusa_sqs_client.expect(:send_message, [], args_verification)
    @medusa_sqs.send_medusa_message(file_id, checksum, found, status, error_message)
    assert_mock(@mock_medusa_sqs_client)
  end
end

