# frozen_string_literal: true
# require 'test/unit'
require 'minitest/autorun'
require 'aws-sdk-dynamodb'
require 'config'

require_relative '../lib/fixity/dynamodb'
class TestDynamodb < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))
  def setup

  end

  def test_put_item_format
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {table_name: "TestTable",
                         item: {Settings.aws.dynamo_db.s3_key => "123/test.tst"}}
    mock_dynamodb_client.expect(:put_item, [], [args_verification])
    dynamodb.put_item("TestTable",
                         {Settings.aws.dynamo_db.s3_key => "123/test.tst"},)
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_get_put_requests_returns_array_of_arrays
    test_batch = %w[1 2 3]
    test_batch_items = Dynamodb.get_put_requests(test_batch)
    assert_instance_of(Array, test_batch_items)
    assert_instance_of(Array, test_batch_items[0])
  end

  def test_get_put_requests_returns_correct_format
    test_batch = %w[1 2 3]
    test_batch_items = Dynamodb.get_put_requests(test_batch)
    expected = [[{put_request: {item:"1"}},{put_request: {item:"2"}},{put_request: {item:"3"}}]]
    assert_equal(expected, test_batch_items)
  end

  def test_get_put_requests_returns_arrays_with_less_than_26_elements
    test_batch = %w[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26]
    test_batch_items = Dynamodb.get_put_requests(test_batch)
    test_batch_items.each do |test_batch_item|
      assert_operator test_batch_item.size, :<=, 25
    end
  end

  def test_get_put_requests_returns_multiple_arrays_when_more_than_26_elements
    test_batch = %w[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26]
    test_batch_items = Dynamodb.get_put_requests(test_batch)
    assert_equal(test_batch_items.size, 2)
  end

  def test_batch_write_items_returns_nil_when_write_requests_nil
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    batch_write_resp = dynamodb.batch_write_items("TestTable", nil)
    assert_nil(batch_write_resp)
  end

  def test_batch_write_items_returns_nil_when_write_requests_empty
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    batch_write_resp = dynamodb.batch_write_items("TestTable", [])
    assert_nil(batch_write_resp)
  end

  def test_batch_write_items_formats_one_array
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {request_items: {"TestTable" => %w[1 2 3] }}
    mock_dynamodb_client.expect(:batch_write_item, [], [args_verification])
    dynamodb.batch_write_items("TestTable", [%w[1 2 3]])
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_batch_write_items_formats_multiple_arrays
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification_1 = {request_items: {"TestTable" => %w[1 2 3] }}
    mock_dynamodb_client.expect(:batch_write_item, [], [args_verification_1])
    args_verification_2 = {request_items: {"TestTable" => %w[4 5 6] }}
    mock_dynamodb_client.expect(:batch_write_item, [], [args_verification_2])
    dynamodb.batch_write_items("TestTable", [%w[1 2 3], %w[4 5 6]])
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_update_item_format
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {table_name: "TestTable",
                         key: {Settings.aws.dynamo_db.s3_key => "123/test.tst"},
                         expression_attribute_names: {"#TV" => "testVal"},
                         expression_attribute_values: {":test_value" => "testValue"},
                         update_expression: "SET #TV = :restoration_status",
                         return_values: "ALL_OLD" }

    mock_dynamodb_client.expect(:update_item, [], [args_verification])
    dynamodb.update_item("TestTable",
                    {Settings.aws.dynamo_db.s3_key => "123/test.tst"},
           {"#TV" => "testVal"},
          {":test_value" => "testValue"},
              "SET #TV = :restoration_status",
                  "ALL_OLD")
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_update_item_default_ret_val_param
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {table_name: "TestTable",
                         key: {Settings.aws.dynamo_db.s3_key => "123/test.tst"},
                         expression_attribute_names: {"#TV" => "testVal"},
                         expression_attribute_values: {":test_value" => "testValue"},
                         update_expression: "SET #TV = :restoration_status",
                         return_values: "NONE" }

    mock_dynamodb_client.expect(:update_item, [], [args_verification])
    dynamodb.update_item("TestTable",
                         {Settings.aws.dynamo_db.s3_key => "123/test.tst"},
                         {"#TV" => "testVal"},
                         {":test_value" => "testValue"},
                         "SET #TV = :restoration_status")
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_query_with_index_format
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {table_name: "TestTable",
                         index_name: "TestIndex",
                         limit: 1,
                         scan_index_forward: true,
                         expression_attribute_values: {":s3_key" => "123/test.tst",},
                         key_condition_expression: "#{Settings.aws.dynamo_db.s3_key} = :s3_key",}
    mock_dynamodb_client.expect(:query, [], [args_verification])
    dynamodb.query_with_index("TestTable",
                              "TestIndex",
                              1,
                              { ":s3_key" => "123/test.tst",},
                              "#{Settings.aws.dynamo_db.s3_key} = :s3_key")
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_query_format
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {table_name: "TestTable",
                         limit: 1,
                         scan_index_forward: true,
                         expression_attribute_values: {":s3_key" => "123/test.tst",},
                         key_condition_expression: "#{Settings.aws.dynamo_db.s3_key} = :s3_key",}
    mock_dynamodb_client.expect(:query, [], [args_verification])
    dynamodb.query("TestTable",
                              1,
                              { ":s3_key" => "123/test.tst",},
                              "#{Settings.aws.dynamo_db.s3_key} = :s3_key")
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_scan_format
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {table_name: "TestTable",
                         limit: 1}
    mock_dynamodb_client.expect(:scan, [], [args_verification])
    dynamodb.scan("TestTable",
                   1,)
    assert_equal(mock_dynamodb_client.verify, true)
  end

  def test_delete_item_format
    mock_dynamodb_client = Minitest::Mock.new
    dynamodb = Dynamodb.new(mock_dynamodb_client)
    args_verification = {key: {Settings.aws.dynamo_db.s3_key => "123/test.tst"},
                         table_name: "TestTable"}
    mock_dynamodb_client.expect(:delete_item, [], [args_verification])
    dynamodb.delete_item({Settings.aws.dynamo_db.s3_key => "123/test.tst"},
                         "TestTable")
    assert_equal(mock_dynamodb_client.verify, true)
  end
end
