require 'minitest/autorun'
require 'aws-sdk-s3'
require 'config'

require_relative '../lib/fixity/s3'

class TestS3 < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", 'test'))

  def setup
    @mock_s3_client = Minitest::Mock.new
    @s3 = S3.new(@mock_s3_client)
  end

  def test_put_object
    body = 'testBody'
    bucket = Settings.aws.s3.backup_bucket
    key = '123/test.tst'
    args_verification = [body: body, bucket: bucket, key: key]
    @mock_s3_client.expect(:put_object, [], args_verification)
    @s3.put_object(body, bucket, key)
    assert_mock(@mock_s3_client)
  end

  def test_get_object_to_response_target
    bucket = Settings.aws.s3.backup_bucket
    key = '123/test.tst'
    response_target = '~/test/test-target.tst'
    args_verification = [bucket: bucket, key: key, response_target: response_target]
    @mock_s3_client.expect(:get_object, [], args_verification)
    @s3.get_object_to_response_target(bucket, key, response_target)
    assert_mock(@mock_s3_client)
  end

  def test_get_object
    bucket = Settings.aws.s3.backup_bucket
    key = '123/test.tst'
    args_verification = [bucket: bucket, key: key]
    @mock_s3_client.expect(:get_object, [], args_verification)
    @s3.get_object(bucket, key)
    assert_mock(@mock_s3_client)
  end

  def test_get_object_with_byte_range
    bucket = Settings.aws.s3.backup_bucket
    key = '123/test.tst'
    range = 'bytes=0-12345678'
    args_verification = [bucket: bucket, key: key, range: range]
    @mock_s3_client.expect(:get_object, [], args_verification)
    @s3.get_object_with_byte_range(bucket, key, range)
    assert_mock(@mock_s3_client)
  end

  def test_restore_object
    mock_dynamodb = Minitest::Mock.new
    bucket = Settings.aws.s3.backup_bucket
    key = '123/test.tst'
    file_id = '123'
    job_params = { tier: Settings.aws.s3.bulk }
    restore_request = { days: 1, glacier_job_parameters: job_params }
    args_verification = [bucket: bucket, key: key, restore_request: restore_request]
    @mock_s3_client.expect(:restore_object, [], args_verification)
    @s3.restore_object(mock_dynamodb, bucket, key, file_id)
    assert_mock(@mock_s3_client)
  end

  def test_restore_object_exception
    mock_dynamodb = Minitest::Mock.new
    bucket = Settings.aws.s3.backup_bucket
    key = '123/test.tst'
    file_id = '123'
    job_params = { tier: 'IntentionallyWrongTier' }
    restore_request = { days: 1, glacier_job_parameters: job_params }
    args_verification = [bucket: bucket, key: key, restore_request: restore_request]
    error_message = '#<MockExpectationError: mocked method :restore_object called with unexpected arguments'\
                    ' [{:bucket=>"medusa-test-main-backup", :key=>"123/test.tst", '\
                    ':restore_request=>{:days=>1, :glacier_job_parameters=>{:tier=>"Bulk"}}}]>'
    item = {
      Settings.aws.dynamodb.s3_key => key,
      Settings.aws.dynamodb.file_id => file_id,
      Settings.aws.dynamodb.message => error_message,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    @mock_s3_client.expect(:restore_object, [], args_verification)
    mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.restoration_errors_table_name, item])
    Time.stub(:now, Time.new(1)) do
      # s3_client.stub(:restore_object, raises_exception) do
      @s3.restore_object(mock_dynamodb, bucket, key, file_id)
      assert_mock(mock_dynamodb)
      # end
    end
  end

  def test_found?
    mock_s3_resource = Minitest::Mock.new
    mock_bucket = Minitest::Mock.new
    bucket = Settings.aws.s3.backup_bucket
    key = '123/%23test.tst'
    key_unesc = '123/#test.tst'
    mock_object = Minitest::Mock.new
    mock_s3_resource.expect(:bucket, mock_bucket, [bucket])
    mock_bucket.expect(:object, mock_object, [key_unesc])
    mock_object.expect(:exists?, true)
    found = @s3.found?(bucket, key, mock_s3_resource)
    assert_equal(true, found)
    assert_mock(mock_s3_resource)
    assert_mock(mock_bucket)
    assert_mock(mock_object)
  end
end
