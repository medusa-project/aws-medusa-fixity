require 'minitest/autorun'
require 'config'

require_relative '../lib/restoration_event'
require_relative '../lib/fixity/dynamodb'

class TestRestorationEvent < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))

  def setup
    @mock_s3 = Minitest::Mock.new
    @mock_dynamodb = Minitest::Mock.new
    @mock_sqs = Minitest::Mock.new
    @restoration_event = RestorationEvent.new(@mock_s3, @mock_dynamodb, @mock_sqs)
  end

  def test_handle_message_completed
    mock_response = Minitest::Mock.new
    mock_data = Minitest::Mock.new
    mock_messages = Minitest::Mock.new
    mock_message = Minitest::Mock.new
    receive_args_verification = [{queue_url: Settings.aws.sqs.s3_queue_url,
                                  max_number_of_messages: 10,
                                  visibility_timeout: 300}]
    @mock_sqs.expect(:receive_message, mock_response, receive_args_verification)
    mock_response.expect(:data, mock_data)
    mock_data.expect(:messages, mock_messages)
    mock_messages.expect(:count, 1)
    mock_response.expect(:messages, [mock_message])
    mock_message.expect(:body, File.read("test/s3_restore_completed.json"))
    mock_message.expect(:receipt_handle, "123")
    delete_args_verification = [{queue_url: Settings.aws.sqs.s3_queue_url, receipt_handle: "123"}]
    @mock_sqs.expect(:delete_message, nil, delete_args_verification)
    key = { Settings.aws.dynamodb.s3_key => "123/test.txt" }
    expr_attr_values= {
      ":restoration_status" => Settings.aws.dynamodb.completed,
      ":fixity_ready" => Settings.aws.dynamodb.true,
      ":file_size" => "123456",
      ":timestamp" => "1970-01-01T00:00:00.000Z"
    }
    update_expr = "SET #{Settings.aws.dynamodb.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamodb.fixity_ready} = :fixity_ready, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamodb.file_size} = :file_size"
    dynamodb_args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item, nil, dynamodb_args_verification)
    @restoration_event.handle_message
    assert_mock(@mock_sqs)
    assert_mock(@mock_dynamodb)
  end

  def test_handle_message_delete
    mock_response = Minitest::Mock.new
    mock_data = Minitest::Mock.new
    mock_messages = Minitest::Mock.new
    mock_message = Minitest::Mock.new
    receive_args_verification = [{queue_url: Settings.aws.sqs.s3_queue_url,
                                  max_number_of_messages: 10,
                                  visibility_timeout: 300}]
    @mock_sqs.expect(:receive_message, mock_response, receive_args_verification)
    mock_response.expect(:data, mock_data)
    mock_data.expect(:messages, mock_messages)
    mock_messages.expect(:count, 1)
    mock_response.expect(:messages, [mock_message])
    mock_message.expect(:body, File.read("test/s3_restore_delete.json"))
    mock_message.expect(:receipt_handle, "123")
    delete_args_verification = [{queue_url: Settings.aws.sqs.s3_queue_url, receipt_handle: "123"}]
    @mock_sqs.expect(:delete_message, nil, delete_args_verification)
    key = { Settings.aws.dynamodb.s3_key => "123/test.txt" }
    expr_attr_values = {
      ":restoration_status" => Settings.aws.dynamodb.expired,
      ":file_size" => "123456",
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.restoration_status} = :restoration_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, "\
                      "#{Settings.aws.dynamodb.file_size} = :file_size "\
                  "REMOVE #{Settings.aws.dynamodb.fixity_ready}"
    ret_val =  "ALL_OLD"
    dynamodb_args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr, ret_val]
    @mock_dynamodb.expect(:update_item, nil, dynamodb_args_verification)
    Time.stub(:now, Time.new(2)) do
      @restoration_event.handle_message
      assert_mock(@mock_sqs)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_handle_message_returns_nil_when_no_messages_available
    mock_response = Minitest::Mock.new
    mock_data = Minitest::Mock.new
    mock_messages = Minitest::Mock.new
    receive_args_verification = [{queue_url: Settings.aws.sqs.s3_queue_url,
                                  max_number_of_messages: 10,
                                  visibility_timeout: 300}]
    @mock_sqs.expect(:receive_message, mock_response, receive_args_verification)
    mock_response.expect(:data, mock_data)
    mock_data.expect(:messages, mock_messages)
    mock_messages.expect(:count, 0)
    resp = @restoration_event.handle_message
    assert_nil(resp)
  end

  def test_handle_completed
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
    @mock_dynamodb.expect(:update_item, [], args_verification)
    @restoration_event.handle_completed(test_key, file_size, timestamp)
    assert_mock(@mock_dynamodb)
  end

  def test_handle_deleted
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
    @mock_dynamodb.expect(:update_item, nil, args_verification)
    Time.stub(:now, Time.new(2)) do
      @restoration_event.handle_deleted(test_key, file_size)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_handle_expiration_expired
    update_item_resp = Object.new
    def update_item_resp.attributes = {Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.calculating,
                                       Settings.aws.dynamodb.s3_key => "123/test.tst",
                                       Settings.aws.dynamodb.file_id => "123",
                                       Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012"}
    s3_args_validation = [@mock_dynamodb, Settings.aws.s3.backup_bucket, "123/test.tst", 123]
    @mock_s3.expect(:restore_object, [], s3_args_validation)
    item = {
      Settings.aws.dynamodb.s3_key => "123/test.tst",
      Settings.aws.dynamodb.file_id => 123,
      Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012",
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3)
    }
    dynamodb_args_validation = [Settings.aws.dynamodb.fixity_table_name, item]
    @mock_dynamodb.expect(:put_item, [], dynamodb_args_validation)
    Time.stub(:now, Time.new(2)) do
      @restoration_event.handle_expiration(update_item_resp)
      assert_mock(@mock_s3)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_handle_expiration_done
    update_item_resp = Object.new
    def update_item_resp.attributes = {Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.done,
                                       Settings.aws.dynamodb.s3_key => "123/test.tst",
                                       Settings.aws.dynamodb.file_id => "123",
                                       Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012"}
    resp = @restoration_event.handle_expiration(update_item_resp)
    assert_equal(false, resp)
  end

  def test_handle_expiration_error
    update_item_resp = Object.new
    def update_item_resp.attributes = {Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.error,
                                       Settings.aws.dynamodb.s3_key => "123/test.tst",
                                       Settings.aws.dynamodb.file_id => "123",
                                       Settings.aws.dynamodb.initial_checksum => "12345678901234567890123456789012"}
    @mock_s3.expect(:restore_object, [], )
    resp = @restoration_event.handle_expiration(update_item_resp)
    assert_equal(false, resp)
  end
end