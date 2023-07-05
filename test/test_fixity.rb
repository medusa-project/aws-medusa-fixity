require 'minitest/autorun'
require 'config'

require_relative '../lib/fixity'
require_relative '../lib/fixity/dynamodb'
class TestFixity < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))
  def test_get_fixity_item_params
    mock_dynamodb = Minitest::Mock.new
    table_name = Settings.aws.dynamo_db.fixity_table_name
    index_name = Settings.aws.dynamo_db.index_name
    limit = 1
    expr_attr_vals = {":ready" => Settings.aws.dynamo_db.true,}
    key_cond_expr = "#{Settings.aws.dynamo_db.fixity_ready} = :ready"
    args_verification = [table_name, index_name, limit, expr_attr_vals, key_cond_expr]
    mock_dynamodb.expect(:query_with_index, [], args_verification)
    Fixity.get_fixity_item(mock_dynamodb)
    assert_mock(mock_dynamodb)
  end

  def test_get_fixity_item
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    item = {"TestItem" => "TestValue" }
    query_resp = Object.new
    def query_resp.items =  [{"TestItem" => "TestValue" }]
    def query_resp.empty? =  false
    dynamodb.stub(:query_with_index, query_resp) do
      resp = Fixity.get_fixity_item(dynamodb)
      assert_equal(item, resp)
    end
  end

  def test_get_fixity_item_returns_nil_if_empty_or_nil
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    query_resp = Object.new
    def query_resp.items =  [{"TestItem" => "TestValue" }]
    def query_resp.empty? =  true
    dynamodb.stub(:query_with_index, nil) do
      resp = Fixity.get_fixity_item(dynamodb)
      assert_nil(resp)
    end
    dynamodb.stub(:query_with_index, query_resp) do
      resp = Fixity.get_fixity_item(dynamodb)
      assert_nil(resp)
    end
  end

  def test_get_fixity_batch_params
    mock_dynamodb = Minitest::Mock.new
    table_name = Settings.aws.dynamo_db.fixity_table_name
    index_name = Settings.aws.dynamo_db.index_name
    limit = 25
    expr_attr_vals = {":ready" => Settings.aws.dynamo_db.true,}
    key_cond_expr = "#{Settings.aws.dynamo_db.fixity_ready} = :ready"
    args_verification = [table_name, index_name, limit, expr_attr_vals, key_cond_expr]
    mock_dynamodb.expect(:query_with_index, [], args_verification)
    Fixity.get_fixity_batch(mock_dynamodb)
    assert_mock(mock_dynamodb)
  end

  def test_get_fixity_batch
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    item_array = [{"TestItem" => "TestValue" }, {"TestItem1" => "TestValue1" }]
    query_resp = Object.new
    def query_resp.items =  [{"TestItem" => "TestValue" }, {"TestItem1" => "TestValue1" }]
    def query_resp.empty? =  false
    dynamodb.stub(:query_with_index, query_resp) do
      resp = Fixity.get_fixity_batch(dynamodb)
      assert_equal(item_array, resp)
    end
  end

  def test_get_fixity_batch_returns_nil_if_empty_or_nil
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    query_resp = Object.new
    def query_resp.items =  [{"TestItem" => "TestValue" }]
    def query_resp.empty? =  true
    dynamodb.stub(:query_with_index, nil) do
      resp = Fixity.get_fixity_batch(dynamodb)
      assert_nil(resp)
    end
    dynamodb.stub(:query_with_index, query_resp) do
      resp = Fixity.get_fixity_batch(dynamodb)
      assert_nil(resp)
    end
  end

  def test_get_fixity_batch_returns_nil_if_items_nil
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    query_resp = Object.new
    def query_resp.items =  nil
    def query_resp.empty? =  false
    dynamodb.stub(:query_with_index, query_resp) do
      resp = Fixity.get_fixity_batch(dynamodb)
      assert_nil(resp)
    end
  end

  def test_get_update_fixity_ready_batch_returns_array_of_arrays
    fixity_batch = [{Settings.aws.dynamo_db.s3_key => "123/test.tst",
                     Settings.aws.dynamo_db.last_updated => Time.new(0).getutc.iso8601(3),
                     Settings.aws.dynamo_db.restoration_status => Settings.aws.dynamo_db.completed,
                     Settings.aws.dynamo_db.fixity_ready => Settings.aws.dynamo_db.true},
                    {Settings.aws.dynamo_db.s3_key => "456/test.tst",
                     Settings.aws.dynamo_db.last_updated => Time.new(1).getutc.iso8601(3),
                     Settings.aws.dynamo_db.restoration_status => Settings.aws.dynamo_db.completed,
                     Settings.aws.dynamo_db.fixity_ready => Settings.aws.dynamo_db.true}]
    updated_batch = Fixity.get_update_fixity_ready_batch(fixity_batch)
    assert_instance_of(Array, updated_batch)
    assert_instance_of(Array, updated_batch[0])
  end

  def test_get_update_fixity_ready
    fixity_batch = [{Settings.aws.dynamo_db.s3_key => "123/test.tst",
                     Settings.aws.dynamo_db.last_updated => Time.new(0).getutc.iso8601(3),
                     Settings.aws.dynamo_db.restoration_status => Settings.aws.dynamo_db.completed,
                     Settings.aws.dynamo_db.fixity_ready => Settings.aws.dynamo_db.true},
                    {Settings.aws.dynamo_db.s3_key => "456/test.tst",
                     Settings.aws.dynamo_db.last_updated => Time.new(1).getutc.iso8601(3),
                     Settings.aws.dynamo_db.restoration_status => Settings.aws.dynamo_db.completed,
                     Settings.aws.dynamo_db.fixity_ready => Settings.aws.dynamo_db.true}]
    expected_batch = [[{put_request:{item:{Settings.aws.dynamo_db.s3_key => "123/test.tst",
                       Settings.aws.dynamo_db.last_updated => Time.new(2).getutc.iso8601(3),
                       Settings.aws.dynamo_db.restoration_status => Settings.aws.dynamo_db.completed,
                       Settings.aws.dynamo_db.fixity_status => Settings.aws.dynamo_db.calculating}}},
                      {put_request:{item:{Settings.aws.dynamo_db.s3_key => "456/test.tst",
                       Settings.aws.dynamo_db.last_updated => Time.new(2).getutc.iso8601(3),
                       Settings.aws.dynamo_db.restoration_status => Settings.aws.dynamo_db.completed,
                       Settings.aws.dynamo_db.fixity_status => Settings.aws.dynamo_db.calculating}}}]]

    Time.stub(:now, Time.new(2)) do
      updated_batch = Fixity.get_update_fixity_ready_batch(fixity_batch)
      assert_equal(expected_batch, updated_batch)
    end
  end

  def test_update_fixity_ready_params
    mock_dynamodb = Minitest::Mock.new
    test_key = "123/test.tst"
    table_name = Settings.aws.dynamo_db.fixity_table_name
    key = { Settings.aws.dynamo_db.s3_key => test_key }
    expr_attr_vals = { ":fixity_status" => Settings.aws.dynamo_db.calculating,
                       ":timestamp" => Time.now.getutc.iso8601(3)}
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp "\
                  "REMOVE #{Settings.aws.dynamo_db.fixity_ready}"
    args_verification = [table_name, key, {}, expr_attr_vals, update_expr]
    mock_dynamodb.expect(:update_item, [], args_verification)
    Fixity.update_fixity_ready(mock_dynamodb, test_key)
    assert_mock(mock_dynamodb)
  end

  def test_calculate_checksum
    #refactor to pass in s3_client
    mock_s3_client = Minitest::Mock.new
    s3 = S3.new(mock_s3_client)
    object_part = Object.new
    def object_part.body = IO.new(IO.sysopen("./.ruby-version", "r"), "r")
    mock_dynamodb = Minitest::Mock.new
    test_key = "123/test.tst"
    file_size = 12345

    s3.stub(:get_object_with_byte_range, object_part) do
      checksum = Fixity.calculate_checksum(s3, test_key, 123, file_size, mock_dynamodb)
      assert_equal("79a84828694ed3ed5482b6d33dea7dd7", checksum)
    end
  end

  # def test_calculate_checksum_error
  #   mock_s3 = Minitest::Mock.new
  #   mock_dynamodb = Minitest::Mock.new
  #   test_key = "123/test.tst"
  #   file_size = 12345
  #   range_verification = "bytes=0-16777216"
  #   s3_args_verification = [Settings.aws.s3.backup_bucket, test_key, range_verification]
  #   mock_s3.expect(:get_object_with_byte_range, [], s3_args_verification)
  #   key = { Settings.aws.dynamo_db.s3_key => test_key }
  #   expr_attr_names = {"#E" => Settings.aws.dynamo_db.error}
  #   expr_attr_values = {":error" => Settings.aws.dynamo_db.true,
  #                       ":fixity_status" => Settings.aws.dynamo_db.error,
  #                       ":timestamp" => Time.new(2).getutc.iso8601(3)
  #   }
  #   update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
  #                     "#{Settings.aws.dynamo_db.last_updated} = :timestamp, " \
  #                     "#E = :error"
  #   dynamodb_args_verification = [Settings.aws.dynamo_db.fixity_table_name, key, expr_attr_names, expr_attr_values, update_expr]
  #   mock_dynamodb.expect(:update_item, [], dynamodb_args_verification)
  #   Time.stub(:now, Time.new(2)) do
  #     Fixity.calculate_checksum(mock_s3, test_key, 123, file_size, mock_dynamodb)
  #     assert_equal(mock_s3.verify, true)
  #     assert_equal(mock_dynamodb.verify, true)
  #   end
  # end

  def test_update_fixity_match
    mock_dynamodb = Minitest::Mock.new
    test_key = "123/test.tst"
    test_checksum = "12345678901234567890123456789012"
    key = { Settings.aws.dynamo_db.s3_key => test_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamo_db.done,
      ":fixity_outcome" => Settings.aws.dynamo_db.match,
      ":calculated_checksum" => test_checksum,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamo_db.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp"
    args_verification = [Settings.aws.dynamo_db.fixity_table_name, key, {}, expr_attr_values, update_expr]
    mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      Fixity.update_fixity_match(mock_dynamodb, test_key, test_checksum)
      assert_mock(mock_dynamodb)
    end
  end

  def test_update_fixity_mismatch
    mock_dynamodb = Minitest::Mock.new
    test_key = "123/test.tst"
    test_checksum = "12345678901234567890123456789012"
    key = { Settings.aws.dynamo_db.s3_key => test_key }
    expr_attr_values = {
      ":mismatch" => Settings.aws.dynamo_db.true,
      ":fixity_status" => Settings.aws.dynamo_db.done,
      ":fixity_outcome" => Settings.aws.dynamo_db.mismatch,
      ":calculated_checksum" => test_checksum,
      ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamo_db.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp, " \
                      "#{Settings.aws.dynamo_db.mismatch} = :mismatch"
    args_verification = [Settings.aws.dynamo_db.fixity_table_name, key, {}, expr_attr_values, update_expr]
    mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      Fixity.update_fixity_mismatch(mock_dynamodb, test_key, test_checksum)
      assert_mock(mock_dynamodb)
    end
  end

  def test_update_fixity_error
    mock_dynamodb = Minitest::Mock.new
    test_key = "123/test.tst"
    key = { Settings.aws.dynamo_db.s3_key => test_key }
    expr_attr_names = {"#E" => Settings.aws.dynamo_db.error}
    expr_attr_values = {":error" => Settings.aws.dynamo_db.true,
                        ":fixity_status" => Settings.aws.dynamo_db.error,
                        ":timestamp" => Time.new(2).getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp, " \
                      "#E = :error"
    args_verification = [Settings.aws.dynamo_db.fixity_table_name, key, expr_attr_names, expr_attr_values, update_expr]
    mock_dynamodb.expect(:update_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      Fixity.update_fixity_error(mock_dynamodb, test_key)
      assert_mock(mock_dynamodb)
    end
  end
end