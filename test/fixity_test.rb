require 'minitest/autorun'
require 'config'

require_relative '../lib/fixity'
require_relative '../lib/fixity/dynamodb'
class TestFixity < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))

  def setup
    @mock_s3 = Minitest::Mock.new
    @mock_dynamodb = Minitest::Mock.new
    @mock_medusa_sqs = Minitest::Mock.new
    @fixity = Fixity.new(@mock_s3, @mock_dynamodb, @mock_medusa_sqs)
  end

  def test_run_fixity
    key = { Settings.aws.dynamodb.s3_key => "123/test.txt" }
    checksum = "79a84828694ed3ed5482b6d33dea7dd7"
    table_name = Settings.aws.dynamodb.fixity_table_name
    query_args_verification = [table_name,
                               Settings.aws.dynamodb.index_name,
                               1,
                               {":ready" => Settings.aws.dynamodb.true,},
                               "#{Settings.aws.dynamodb.fixity_ready} = :ready"]
    items = [{Settings.aws.dynamodb.s3_key => "123/test.txt", Settings.aws.dynamodb.file_id => "123",
              Settings.aws.dynamodb.initial_checksum => checksum, Settings.aws.dynamodb.file_size => 123456}]
    mock_query_resp = Minitest::Mock.new
    @mock_dynamodb.expect(:query_with_index, mock_query_resp, query_args_verification)
    mock_query_resp.expect(:nil?, false)
    mock_query_resp.expect(:items, items)
    mock_query_resp.expect(:items, items)
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamodb.calculating,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp "\
                  "REMOVE #{Settings.aws.dynamodb.fixity_ready}"
    update_args_verification = [table_name, key, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item, nil, update_args_verification)
    object_part = Minitest::Mock.new
    object_part.expect(:body, IO.new(IO.sysopen("#{ENV['RUBY_HOME']}/.ruby-version", "r"), "r"))
    s3_args_verification = [Settings.aws.s3.backup_bucket, "123/test.txt", "bytes=0-16777216"]
    @mock_s3.expect(:get_object_with_byte_range, object_part, s3_args_verification)

    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamodb.done,
      ":fixity_outcome" => Settings.aws.dynamodb.match,
      ":calculated_checksum" => checksum,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamodb.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp"
    args_verification = [table_name, key, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item, [], args_verification)
    file_id = 123
    error_message = nil
    args_verification = [file_id, checksum, true, Settings.aws.sqs.success]
    @mock_medusa_sqs.expect(:send_medusa_message, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @fixity.run_fixity
      assert_mock(@mock_dynamodb)
      assert_mock(@mock_s3)
      assert_mock(@mock_medusa_sqs)
    end
  end

  def test_run_fixity_batch

  end

  def test_get_fixity_item
    item = {"TestItem" => "TestValue" }
    query_resp = Object.new
    def query_resp.items =  [{"TestItem" => "TestValue" }]
    def query_resp.empty? =  false
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, Settings.aws.dynamodb.index_name, 1, expr_attr_vals, key_cond_expr]
    @mock_dynamodb.expect(:query_with_index, query_resp, args_verification)
    resp = @fixity.get_fixity_item
    assert_equal(item, resp)
    assert_mock(@mock_dynamodb)
  end

  def test_get_fixity_item_returns_nil_if_empty_or_nil
    query_resp = Object.new
    def query_resp.items =  []
    def query_resp.empty? =  true
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, Settings.aws.dynamodb.index_name, 1, expr_attr_vals, key_cond_expr]
    @mock_dynamodb.expect(:query_with_index, nil, args_verification)
    resp = @fixity.get_fixity_item
    assert_nil(resp)
    @mock_dynamodb.expect(:query_with_index, query_resp, args_verification )
    resp = @fixity.get_fixity_item
    assert_nil(resp)
  end

  def test_get_fixity_batch
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.index_name
    limit = 25
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    args_verification = [table_name, index_name, limit, expr_attr_vals, key_cond_expr]
    item_array = [{"TestItem" => "TestValue" }, {"TestItem1" => "TestValue1" }]
    query_resp = Object.new
    def query_resp.items =  [{"TestItem" => "TestValue" }, {"TestItem1" => "TestValue1" }]
    def query_resp.empty? =  false
    @mock_dynamodb.expect(:query_with_index, query_resp, args_verification)
    resp = @fixity.get_fixity_batch
    assert_equal(item_array, resp)
  end

  def test_get_fixity_batch_returns_nil_if_empty_or_nil
    query_resp = Object.new
    def query_resp.items =  [{"TestItem" => "TestValue" }]
    def query_resp.empty? =  true
    query_resp_empty = Object.new
    def query_resp_empty.items =  []
    def query_resp_empty.empty? =  false
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.index_name
    limit = 25
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    args_verification = [table_name, index_name, limit, expr_attr_vals, key_cond_expr]
    @mock_dynamodb.expect(:query_with_index, nil, args_verification)
    resp = @fixity.get_fixity_batch
    assert_nil(resp)
    @mock_dynamodb.expect(:query_with_index, query_resp, args_verification)
    resp = @fixity.get_fixity_batch
    assert_nil(resp)
    @mock_dynamodb.expect(:query_with_index, query_resp_empty, args_verification)
    resp = @fixity.get_fixity_batch
    assert_nil(resp)
  end


  def test_get_update_fixity_ready_batch_returns_array_of_arrays
    fixity_batch = [{Settings.aws.dynamodb.s3_key => "123/test.tst",
                     Settings.aws.dynamodb.last_updated => Time.new(0).getutc.iso8601(3),
                     Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.completed,
                     Settings.aws.dynamodb.fixity_ready => Settings.aws.dynamodb.true},
                    {Settings.aws.dynamodb.s3_key => "456/test.tst",
                     Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3),
                     Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.completed,
                     Settings.aws.dynamodb.fixity_ready => Settings.aws.dynamodb.true}]
    updated_batch = @fixity.get_update_fixity_ready_batch(fixity_batch)
    assert_instance_of(Array, updated_batch)
    assert_instance_of(Array, updated_batch[0])
  end

  def test_get_update_fixity_ready
    fixity_batch = [{Settings.aws.dynamodb.s3_key => "123/test.tst",
                     Settings.aws.dynamodb.last_updated => Time.new(0).getutc.iso8601(3),
                     Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.completed,
                     Settings.aws.dynamodb.fixity_ready => Settings.aws.dynamodb.true},
                    {Settings.aws.dynamodb.s3_key => "456/test.tst",
                     Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3),
                     Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.completed,
                     Settings.aws.dynamodb.fixity_ready => Settings.aws.dynamodb.true}]
    expected_batch = [[{put_request:{item:{Settings.aws.dynamodb.s3_key => "123/test.tst",
                       Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3),
                       Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.completed,
                       Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.calculating}}},
                      {put_request:{item:{Settings.aws.dynamodb.s3_key => "456/test.tst",
                       Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3),
                       Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.completed,
                       Settings.aws.dynamodb.fixity_status => Settings.aws.dynamodb.calculating}}}]]

    Time.stub(:now, Time.new(2)) do
      updated_batch = @fixity.get_update_fixity_ready_batch(fixity_batch)
      assert_equal(expected_batch, updated_batch)
    end
  end

  def test_update_fixity_ready_params
    test_key = "123/test.tst"
    table_name = Settings.aws.dynamodb.fixity_table_name
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_vals = { ":fixity_status" => Settings.aws.dynamodb.calculating,
                       ":timestamp" => Time.new(2).getutc.iso8601(3)}
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp "\
                  "REMOVE #{Settings.aws.dynamodb.fixity_ready}"
    args_verification = [table_name, key, expr_attr_vals, update_expr]
    @mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @fixity.update_fixity_ready(test_key)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_calculate_checksum
    object_part = Object.new
    def object_part.body = IO.new(IO.sysopen("#{ENV['RUBY_HOME']}/.ruby-version", "r"), "r")
    mock_dynamodb = Minitest::Mock.new
    test_key = "123/test.tst"
    file_size = 12345
    args_verification = [Settings.aws.s3.backup_bucket, test_key, "bytes=0-16777216"]
    @mock_s3.expect(:get_object_with_byte_range, object_part, args_verification)
    checksum, error_message = @fixity.calculate_checksum(test_key, 123, file_size)
    assert_equal("79a84828694ed3ed5482b6d33dea7dd7", checksum)
    assert_nil(error_message)
  end

  def test_calculate_checksum_character_unescaping
    object_part = Object.new
    def object_part.body = IO.new(IO.sysopen("#{ENV['RUBY_HOME']}/.ruby-version", "r"), "r")
    test_key = "123/%7Etest.tst"
    test_key_unescaped = FixityUtils.unescape(test_key)
    file_size = 12345
    range = "bytes=0-16777216"
    args_verification = [Settings.aws.s3.backup_bucket, test_key_unescaped, range]
    @mock_s3.expect(:get_object_with_byte_range, object_part, args_verification)
    checksum = @fixity.calculate_checksum(test_key, 123, file_size)
    assert_mock(@mock_s3)
  end

  def test_calculate_checksum_file_size_nil
    test_key = "123/%7Etest.tst"
    file_id = 123
    file_size = nil
    error_message_exp = "Error calculating md5 for object #{test_key} with ID #{file_id}: file size nil"
    checksum, error_message = @fixity.calculate_checksum(test_key, 123, file_size)
    assert_nil(checksum)
    assert_equal(error_message_exp, error_message)
  end

  def test_calculate_checksum_error
    test_key = "123/test.tst"
    file_size = 12345
    range_verification = "bytes=0-16777216"
    s3_args_verification = [Settings.aws.s3.backup_bucket, test_key, range_verification]
    @mock_s3.expect(:get_object_with_byte_range, [], s3_args_verification)
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_names = {"#E" => Settings.aws.dynamodb.error}
    expr_attr_values = {":error" => Settings.aws.dynamodb.true,
                        ":fixity_status" => Settings.aws.dynamodb.error,
                        ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, " \
                      "#E = :error"
    dynamodb_args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_names, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item_with_names, [], dynamodb_args_verification)
    Time.stub(:now, Time.new(2)) do
      checksum, error_message = @fixity.calculate_checksum(test_key, 123, file_size)
      assert_mock(@mock_s3)
      assert_mock(@mock_dynamodb)
      assert_nil(checksum)
    end
  end

  def test_handle_outcome_match
    initial_checksum = "12345678901234567890123456789012"
    calculated_checksum = "12345678901234567890123456789012"
    test_key = "123/test.tst"
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamodb.done,
      ":fixity_outcome" => Settings.aws.dynamodb.match,
      ":calculated_checksum" => calculated_checksum,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamodb.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @fixity.handle_outcome(initial_checksum, calculated_checksum, test_key)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_handle_outcome_mismatch
    initial_checksum = "12345678901234567890123456789012"
    calculated_checksum = "23456789012345678901234567890123"
    test_key = "123/test.tst"
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_values = {
      ":mismatch" => Settings.aws.dynamodb.true,
      ":fixity_status" => Settings.aws.dynamodb.done,
      ":fixity_outcome" => Settings.aws.dynamodb.mismatch,
      ":calculated_checksum" => calculated_checksum,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamodb.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, " \
                      "#{Settings.aws.dynamodb.mismatch} = :mismatch"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @fixity.handle_outcome(initial_checksum, calculated_checksum, test_key)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_update_fixity_match
    test_key = "123/test.tst"
    test_checksum = "12345678901234567890123456789012"
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamodb.done,
      ":fixity_outcome" => Settings.aws.dynamodb.match,
      ":calculated_checksum" => test_checksum,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamodb.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @fixity.update_fixity_match(test_key, test_checksum)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_update_fixity_mismatch
    test_key = "123/test.tst"
    test_checksum = "12345678901234567890123456789012"
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_values = {
      ":mismatch" => Settings.aws.dynamodb.true,
      ":fixity_status" => Settings.aws.dynamodb.done,
      ":fixity_outcome" => Settings.aws.dynamodb.mismatch,
      ":calculated_checksum" => test_checksum,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamodb.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, " \
                      "#{Settings.aws.dynamodb.mismatch} = :mismatch"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @fixity.update_fixity_mismatch(test_key, test_checksum)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_update_fixity_error
    test_key = "123/test.tst"
    key = { Settings.aws.dynamodb.s3_key => test_key }
    expr_attr_names = {"#E" => Settings.aws.dynamodb.error}
    expr_attr_values = {":error" => Settings.aws.dynamodb.true,
                        ":fixity_status" => Settings.aws.dynamodb.error,
                        ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, " \
                      "#E = :error"
    args_verification = [Settings.aws.dynamodb.fixity_table_name, key, expr_attr_names, expr_attr_values, update_expr]
    @mock_dynamodb.expect(:update_item_with_names, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @fixity.update_fixity_error(test_key)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_create_medusa_message
    file_id = 123
    checksum = "12345678912345678912345678912"
    error_message = nil
    args_verification = [file_id, checksum, true, Settings.aws.sqs.success]
    @mock_medusa_sqs.expect(:send_medusa_message, [], args_verification)
    @fixity.create_medusa_message(file_id, checksum, error_message)
    assert_mock(@mock_medusa_sqs)
  end

  def test_create_medusa_message_checksum_nil
    file_id = 123
    checksum = nil
    error_message = "testing error message from fixity"
    args_verification = [file_id, checksum, true, Settings.aws.sqs.failure, error_message]
    @mock_medusa_sqs.expect(:send_medusa_message, [], args_verification)
    @fixity.create_medusa_message(file_id, checksum, error_message)
    assert_mock(@mock_medusa_sqs)
  end
end