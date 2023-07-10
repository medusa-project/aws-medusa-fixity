require 'minitest/autorun'
require 'config'

require_relative '../lib/restoration_event'
require_relative '../lib/fixity/dynamodb'

class TestRestorationEvent < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))
  def test_handle_completed
    mock_dynamodb = Minitest::Mock.new
    test_key = "123/test.tst"
    file_size = 12345
    timestamp = Time.new(1)
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_values= {
      ":restoration_status" => Settings.aws.dynamodb.completed,
      ":fixity_ready" => Settings.aws.dynamodb.true,
      ":file_size" => file_size,
      ":timestamp" => timestamp
    }
    update_expr = "SET #{Settings.aws.dynamodb.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamodb.fixity_ready} = :fixity_ready, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamodb.file_size} = :file_size"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr]
    mock_dynamodb.expect(:update_item, [], args_verification)
    RestorationEvent.handle_completed(mock_dynamodb, test_key, file_size, timestamp)
    assert_mock(mock_dynamodb)
  end

  def test_handle_deleted
    mock_dynamodb = Minitest::Mock.new
    mock_s3 = Minitest::Mock.new
    test_key = "123/test.tst"
    file_size = 12345
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_values = {
      ":restoration_status" => Settings.aws.dynamodb.expired,
      ":file_size" => file_size,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamodb.file_size} = :file_size "\
                  "REMOVE #{Settings.aws.dynamodb.fixity_ready}"
    ret_val = "ALL_OLD"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr, ret_val]
    mock_dynamodb.expect(:update_item, nil, args_verification)
    Time.stub(:now, Time.new(2)) do
      RestorationEvent.handle_deleted(mock_dynamodb, mock_s3, test_key, file_size)
      assert_mock(mock_dynamodb)
    end
  end

  def test_handle_expiration_expired
    update_item_resp = Object.new
    def update_item_resp.attributes = {Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.calculating,
                                       Settings.aws.dynamodb.s3_key => "123/test.tst",
                                       Settings.aws.dynamodb.file_id => "123",
                                       Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012"}
    mock_dynamodb = Minitest::Mock.new
    mock_s3 = Minitest::Mock.new
    s3_args_validation = [Settings.aws.s3.backup_bucket, "123/test.tst"]
    mock_s3.expect(:restore_object, [], s3_args_validation)
    item = {
      Settings.aws.dynamodb.s3_key => "123/test.tst",
      Settings.aws.dynamodb.file_id => 123,
      Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012",
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3)
    }
    dynamodb_args_validation = [Settings.aws.dynamodb.fixity_table_name, item]
    mock_dynamodb.expect(:put_item, [], dynamodb_args_validation)
    Time.stub(:now, Time.new(2)) do
      RestorationEvent.handle_expiration(mock_dynamodb, mock_s3, update_item_resp)
      assert_mock(mock_s3)
      assert_mock(mock_dynamodb)
    end
  end

  def test_handle_expiration_done
    update_item_resp = Object.new
    def update_item_resp.attributes = {Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.done,
                                       Settings.aws.dynamodb.s3_key => "123/test.tst",
                                       Settings.aws.dynamodb.file_id => "123",
                                       Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012"}
    mock_dynamodb = Minitest::Mock.new
    mock_s3 = Minitest::Mock.new
    resp = RestorationEvent.handle_expiration(mock_dynamodb, mock_s3, update_item_resp)
    assert_equal(false, resp)
  end

  def test_handle_expiration_error
    update_item_resp = Object.new
    def update_item_resp.attributes = {Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.error,
                                       Settings.aws.dynamodb.s3_key => "123/test.tst",
                                       Settings.aws.dynamodb.file_id => "123",
                                       Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012"}
    mock_dynamodb = Minitest::Mock.new
    mock_s3 = Minitest::Mock.new
    resp = RestorationEvent.handle_expiration(mock_dynamodb, mock_s3, update_item_resp)
    assert_equal(false, resp)
  end
end