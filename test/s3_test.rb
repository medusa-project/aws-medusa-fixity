require 'minitest/autorun'
require 'aws-sdk-s3'
require 'config'

require_relative '../lib/fixity/s3'

class TestS3 < Minitest::Test

  def test_put_object
    mock_s3_client = Minitest::Mock.new
    s3 = S3.new(mock_s3_client)
    body = "testBody"
    bucket = Settings.aws.s3.backup_bucket
    key = "123/test.tst"
    args_verification = [body: body, bucket: bucket, key: key]
    mock_s3_client.expect(:put_object, [], args_verification)
    s3.put_object(body, bucket, key)
    assert_mock(mock_s3_client)
  end

  def test_get_object_to_response_target
    mock_s3_client = Minitest::Mock.new
    s3 = S3.new(mock_s3_client)
    bucket = Settings.aws.s3.backup_bucket
    key = "123/test.tst"
    response_target = "~/test/test-target.tst"
    args_verification = [bucket: bucket, key: key, response_target: response_target]
    mock_s3_client.expect(:get_object, [], args_verification)
    s3.get_object_to_response_target(bucket, key, response_target)
    assert_mock(mock_s3_client)
  end

  def test_get_object
    mock_s3_client = Minitest::Mock.new
    s3 = S3.new(mock_s3_client)
    bucket = Settings.aws.s3.backup_bucket
    key = "123/test.tst"
    args_verification = [bucket: bucket, key: key]
    mock_s3_client.expect(:get_object, [], args_verification)
    s3.get_object(bucket, key)
    assert_mock(mock_s3_client)
  end

  def test_get_object_with_byte_range
    mock_s3_client = Minitest::Mock.new
    s3 = S3.new(mock_s3_client)
    bucket = Settings.aws.s3.backup_bucket
    key = "123/test.tst"
    range = "bytes=0-12345678"
    args_verification = [bucket: bucket, key: key, range: range]
    mock_s3_client.expect(:get_object, [], args_verification)
    s3.get_object_with_byte_range(bucket, key, range)
    assert_mock(mock_s3_client)
  end

  def test_restore_object
    mock_s3_client = Minitest::Mock.new
    s3 = S3.new(mock_s3_client)
    bucket = Settings.aws.s3.backup_bucket
    key = "123/test.tst"
    job_params = {tier: Settings.aws.bulk}
    restore_request = {days: 1, glacier_job_parameters: job_params}
    args_verification = [bucket: bucket, key: key, restore_request: restore_request]
    mock_s3_client.expect(:restore_object, [], args_verification)
    s3.restore_object(bucket, key)
    assert_mock(mock_s3_client)
  end

end