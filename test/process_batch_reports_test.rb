require 'minitest/autorun'
require 'config'
require 'json'

require_relative '../lib/process_batch_reports'

class TestProcessBatchReports < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))

  def test_get_job_id_returns_nill
    mock_dynamodb = Minitest::Mock.new
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, 1]
    mock_dynamodb.expect(:scan, nil, args_verification)
    resp = ProcessBatchReports.get_job_id(mock_dynamodb)
    assert_nil(resp)
    assert_mock(mock_dynamodb)
  end

  def test_get_job_id_returns_job_id
    mock_dynamodb = Minitest::Mock.new
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, 1]
    job_id = "job-123456789"
    scan_resp = Object.new
    def scan_resp.items = [{Settings.aws.dynamodb.job_id => "job-123456789"}]
    mock_dynamodb.expect(:scan, scan_resp, args_verification)
    resp = ProcessBatchReports.get_job_id(mock_dynamodb)
    assert_equal(job_id, resp)
    assert_mock(mock_dynamodb)
  end

  def test_job_info
    mock_s3_control = Minitest::Mock.new
    mock_describe_job_response = Minitest::Mock.new
    job_id = "job-123456789"
    mock_s3_control.expect(:describe_job, mock_describe_job_response, [job_id])
    mock_describe_job_response.expect(:nil?, false)
    ProcessBatchReports.get_job_info(mock_s3_control, job_id)
    assert_mock(mock_s3_control)
  end

  def test_job_info_returns_nil
      mock_s3_control = Minitest::Mock.new
      job_id = "job-123456789"
      mock_s3_control.expect(:describe_job, nil, [job_id])
      job_info = ProcessBatchReports.get_job_info(mock_s3_control, job_id)
      assert_mock(mock_s3_control)
      assert_nil(job_info)
  end

  def test_get_duration
    mock_job_info = Minitest::Mock.new
    mock_job = Minitest::Mock.new
    mock_progress_summary = Minitest::Mock.new
    mock_timers = Minitest::Mock.new
    mock_job_info.expect(:job, mock_job)
    mock_job.expect(:progress_summary, mock_progress_summary)
    mock_progress_summary.expect(:timers, mock_timers)
    mock_timers.expect(:elapsed_time_in_active_seconds, 123)
    mock_job_info.expect(:job, mock_job)
    mock_job.expect(:progress_summary, mock_progress_summary)
    mock_progress_summary.expect(:total_number_of_tasks, 12)
    mock_job_info.expect(:job, mock_job)
    mock_job.expect(:job_id, "job-123456")
    ProcessBatchReports.get_duration(mock_job_info)
    assert_mock(mock_job_info)
    assert_mock(mock_job)
    assert_mock(mock_progress_summary)
    assert_mock(mock_timers)
  end

  def test_get_tasks_failed
    mock_job_info = Minitest::Mock.new
    mock_job = Minitest::Mock.new
    mock_progress_summary = Minitest::Mock.new
    mock_job_info.expect(:job, mock_job)
    # mock_describe_job.expect(:status, "Completed")
    mock_job.expect(:progress_summary, mock_progress_summary)
    mock_progress_summary.expect(:number_of_tasks_failed, 1)
    errors = ProcessBatchReports.get_tasks_failed(mock_job_info)
    assert_mock(mock_job_info)
    assert_mock(mock_job)
    assert_mock(mock_progress_summary)
    assert_equal(1, errors)
  end

  def test_get_job_status
    status_exp = "Completed"
    mock_job_info = Minitest::Mock.new
    mock_job = Minitest::Mock.new
    mock_job_info.expect(:job, mock_job)
    mock_job.expect(:status, status_exp)
    status_act = ProcessBatchReports.get_job_status(mock_job_info)
    assert_mock(mock_job_info)
    assert_mock(mock_job)
    assert_equal(status_exp, status_act)
  end

  def test_get_manifest_key
    mock_s3 = Minitest::Mock.new
    mock_s3_resp = Minitest::Mock.new
    mock_body = Minitest::Mock.new
    test_key = "123/test.tst"
    read_resp = {"Results" => [{"Key" => test_key}]}.to_json
    job_id = "job-123456789"
    key = "#{Settings.aws.s3.batch_prefix}/job-#{job_id}/manifest.json"
    args_verification = [Settings.aws.s3.backup_bucket, key]
    mock_s3.expect(:get_object, mock_s3_resp, args_verification)
    mock_s3_resp.expect(:nil?, false)
    mock_s3_resp.expect(:body, mock_body)
    mock_body.expect(:read, read_resp)
    manifest_key = ProcessBatchReports.get_manifest_key(mock_s3, job_id)
    assert_mock(mock_s3)
    assert_equal(test_key, manifest_key)
  end

  def test_parse_completion_report
    mock_dynamodb = Minitest::Mock.new
    mock_s3 = Minitest::Mock.new
    response_target = "./report.csv"
    manifest_key = "test/test-manifest.csv"
    table_name = Settings.aws.dynamodb.fixity_table_name
    limit = 1
    key_1 = "123/test.tst"
    key_2 = "123/test1.tst"
    key_3 = "345/test.tst"
    expr_attr_vals_1 = { ":s3_key" => key_1,}
    expr_attr_vals_2 = { ":s3_key" => key_2,}
    expr_attr_vals_3 = { ":s3_key" => key_3,}
    key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :s3_key"
    query_resp1 = Object.new
    def query_resp1.items = [{Settings.aws.dynamodb.file_id => "1"}]
    query_resp2 = Object.new
    def query_resp2.items = [{Settings.aws.dynamodb.file_id => "2"}]
    query_resp3 = Object.new
    def query_resp3.items = [{Settings.aws.dynamodb.file_id => "3"}]
    dynamodb_args_ver_1 = [table_name, limit, expr_attr_vals_1, key_cond_expr]
    dynamodb_args_ver_2 = [table_name, limit, expr_attr_vals_2, key_cond_expr]
    dynamodb_args_ver_3 = [table_name, limit, expr_attr_vals_3, key_cond_expr]
    mock_dynamodb.expect(:query, query_resp1, dynamodb_args_ver_1)
    mock_dynamodb.expect(:query, query_resp2, dynamodb_args_ver_2)
    mock_dynamodb.expect(:query, query_resp3, dynamodb_args_ver_3)

    s3_args_verification = [Settings.aws.s3.backup_bucket, manifest_key, response_target]
    mock_s3.expect(:get_object_to_response_target, [], s3_args_verification)
    error_hash1 = {
      Settings.aws.dynamodb.s3_key => key_1,
      Settings.aws.dynamodb.file_id => "1",
      Settings.aws.dynamodb.err_code => "409",
      Settings.aws.dynamodb.https_status_code => "RestoreAlreadyInProgress",
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    error_hash2 = {
      Settings.aws.dynamodb.s3_key => key_2,
      Settings.aws.dynamodb.file_id => "2",
      Settings.aws.dynamodb.err_code => "409",
      Settings.aws.dynamodb.https_status_code => "RestoreAlreadyInProgress",
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    error_hash3 = {
      Settings.aws.dynamodb.s3_key => key_3,
      Settings.aws.dynamodb.file_id => "3",
      Settings.aws.dynamodb.err_code => "200",
      Settings.aws.dynamodb.https_status_code => "PermanentFailure",
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    exp_error_batch = [error_hash1, error_hash2, error_hash3]
    Time.stub(:now, Time.new(1)) do
      File.stub(:read, File.read("#{ENV['TEST_HOME']}/report.csv")) do
        error_batch = ProcessBatchReports.parse_completion_report(mock_dynamodb, mock_s3, manifest_key)
        assert_equal(exp_error_batch, error_batch)
      end
    end
    assert_mock(mock_s3)
  end

  def test_get_file_id
    mock_dynamodb = Minitest::Mock.new
    table_name = Settings.aws.dynamodb.fixity_table_name
    query_resp = Object.new
    def query_resp.items = [{Settings.aws.dynamodb.file_id => "123"}]
    limit = 1
    expr_attr_vals = { ":s3_key" => "123/test.tst",}
    key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :s3_key"
    args_verification = [table_name, limit, expr_attr_vals, key_cond_expr]
    mock_dynamodb.expect(:query, query_resp, args_verification)
    file_id = ProcessBatchReports.get_file_id(mock_dynamodb, "123/test.tst")
    assert_mock(mock_dynamodb)
    assert_equal("123", file_id)
  end

  def test_remove_job_id
    mock_dynamodb = Minitest::Mock.new
    job_id = "job-123456789"
    key = { Settings.aws.dynamodb.job_id => job_id,}
    args_verification = [key, Settings.aws.dynamodb.batch_job_ids_table_name]
    mock_dynamodb.expect(:delete_item, [], args_verification)
    ProcessBatchReports.remove_job_id(mock_dynamodb, job_id)
    assert_mock(mock_dynamodb)
  end

end