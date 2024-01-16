# frozen_string_literal: true
require 'test_helper'

class TestProcessResults < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", 'test'))
  S3_KEY = Settings.aws.dynamodb.s3_key
  FILE_ID = Settings.aws.dynamodb.file_id
  INIT_CHECKSUM = Settings.aws.dynamodb.initial_checksum
  CALC_CHECKSUM = Settings.aws.dynamodb.calculated_checksum
  LAST_UPDATED = Settings.aws.dynamodb.last_updated

  def setup
    @mock_dynamodb = Minitest::Mock.new
    @process_results = ProcessResults.new(@mock_dynamodb)
  end

  def test_create_fixity_mismatch_csv
    csv_name_exp = 'fixity-mismatch-0001-01-01.csv'
    headers_exp = [S3_KEY, FILE_ID, INIT_CHECKSUM, CALC_CHECKSUM, LAST_UPDATED]
    Time.stub(:now, Time.new(1)) do
      csv_name_act = @process_results.create_fixity_mismatch_csv
      assert_equal(csv_name_exp, csv_name_act)
      assert_equal(1, File.new(csv_name_act).readlines.length)
      headers_act = CSV.open(csv_name_act, 'r', &:first)
      assert_equal(headers_exp, headers_act)
    end
    File.delete(csv_name_exp)
  end

  def test_mismatch_resp
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.mismatch_index_name
    expr_names = {
      '#S3' => S3_KEY,
      '#FI' => FILE_ID,
      '#IC' => INIT_CHECKSUM,
      '#CC' => CALC_CHECKSUM,
      '#LU' => LAST_UPDATED,
    }
    proj_expr = '#S3, #FI, #IC, #CC, #LU'
    @mock_dynamodb.expect(:scan_index, [], [table_name, index_name, expr_names, proj_expr])
    @process_results.mismatch_resp
    assert_mock(@mock_dynamodb)
  end

  def test_populate_fixity_mismatch_csv
    csv_name = 'fixity-mismatch-0002-01-01.csv'
    mock_response = Minitest::Mock.new
    test_key = 'testKey'
    test_id = '123'
    test_init_checksum = '12345678901234567890123456789012'
    test_calc_checksum = '23456789012345678901234567890123'
    test_last_updated = Time.new(1)
    item = {  S3_KEY => test_key,
              FILE_ID => test_id,
              INIT_CHECKSUM => test_init_checksum,
              CALC_CHECKSUM => test_calc_checksum,
              LAST_UPDATED => test_last_updated }
    mock_response.expect(:items, [item], [])
    @process_results.populate_fixity_mismatch_csv(csv_name, mock_response)
    assert_mock(mock_response)
    assert_equal(1, File.new(csv_name).readlines.length)
    CSV.foreach(csv_name, headers: true).take(1).each do |row|
      key, id, init_check, calc_check, last_updated = row
      assert_equal(test_key, key)
      assert_equal(test_id, id)
      assert_equal(test_init_checksum, init_check)
      assert_equal(test_calc_checksum, calc_check)
      assert_equal(test_last_updated, last_updated)
    end
    File.delete(csv_name)
  end

  def test_generate_fixity_mismatch_csv

    csv_name = 'fixity-mismatch-0003-01-01.csv'
    mock_response = Minitest::Mock.new
    test_key = 'keyTest'
    test_id = '456'
    test_init_checksum = '23456789012345678901234567890123'
    test_calc_checksum = '34567890123456789012345678901234'
    test_last_updated = "#{Time.new(1)}"
    item = {  S3_KEY => test_key,
              FILE_ID => test_id,
              INIT_CHECKSUM => test_init_checksum,
              CALC_CHECKSUM => test_calc_checksum,
              LAST_UPDATED => test_last_updated }
    mock_response.expect(:items, [item], [])
    headers_exp = [S3_KEY, FILE_ID, INIT_CHECKSUM, CALC_CHECKSUM, LAST_UPDATED]
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.mismatch_index_name
    expr_names = {
      '#S3' => S3_KEY,
      '#FI' => FILE_ID,
      '#IC' => INIT_CHECKSUM,
      '#CC' => CALC_CHECKSUM,
      '#LU' => LAST_UPDATED,
    }
    proj_expr = '#S3, #FI, #IC, #CC, #LU'
    @mock_dynamodb.expect(:scan_index, mock_response, [table_name, index_name, expr_names, proj_expr])
    Time.stub(:now, Time.new(3)) do
      @process_results.generate_fixity_mismatch_csv
      assert_mock(@mock_dynamodb)
      assert_mock(mock_response)
      assert_equal(2, File.new(csv_name).readlines.length)
      headers_act = CSV.open(csv_name, 'r', &:first)
      assert_equal(headers_exp, headers_act)
      CSV.foreach(csv_name, headers: true).take(1).each do |row|
        key, id, init_check, calc_check, last_updated = row
        assert_equal(test_key, key[1])
        assert_equal(test_id, id[1])
        assert_equal(test_init_checksum, init_check[1])
        assert_equal(test_calc_checksum, calc_check[1])
        assert_equal(test_last_updated, last_updated[1])
      end
      File.delete(csv_name)
    end
  end
end

