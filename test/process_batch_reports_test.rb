require 'test_helper'

class TestProcessBatchReports < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", 'test'))

  def setup
    @mock_s3 = Minitest::Mock.new
    @mock_dynamodb = Minitest::Mock.new
    @mock_s3_control = Minitest::Mock.new
    @mock_sqs = Minitest::Mock.new
    @p_b_r = ProcessBatchReports.new(@mock_s3, @mock_dynamodb, @mock_s3_control, @mock_sqs)
  end

  def teardown
    File.truncate('logs/fixity.log', 0)
  end

  def test_process_failures
    job_ids_table = Settings.aws.dynamodb.batch_job_ids_table_name
    backup_bucket = Settings.aws.s3.backup_bucket
    fixity_bucket = Settings.aws.s3.fixity_bucket_arn

    # get job id
    args_verification = [job_ids_table, 1]
    job_id = 'job-123456789'
    scan_resp = Minitest::Mock.new
    items = [{ Settings.aws.dynamodb.job_id => job_id }]
    scan_resp.expect(:nil?, false)
    scan_resp.expect(:items, items)
    scan_resp.expect(:items, items)
    @mock_dynamodb.expect(:scan, scan_resp, args_verification)

    # get job info
    job_info = Minitest::Mock.new
    @mock_s3_control.expect(:describe_job, job_info, [job_id])
    job_info.expect(:nil?, false)
    job_info.expect(:nil?, false)

    # get job status
    job = Minitest::Mock.new
    job_info.expect(:job, job)
    job.expect(:status, Settings.aws.s3.complete)

    # get duration
    progress_summary = Minitest::Mock.new
    timers = Minitest::Mock.new
    job_info.expect(:job, job)
    job.expect(:progress_summary, progress_summary)
    progress_summary.expect(:timers, timers)
    timers.expect(:elapsed_time_in_active_seconds, 10)
    job_info.expect(:job, job)
    job.expect(:progress_summary, progress_summary)
    progress_summary.expect(:total_number_of_tasks, 100)
    job_info.expect(:job, job)
    job.expect(:job_id, job_id)

    # get tasks failed
    job_info.expect(:job, job)
    job.expect(:progress_summary, progress_summary)
    progress_summary.expect(:number_of_tasks_failed, 3)

    # get manifest key
    manifest_key = "#{job_id}/results/12345678912345678912345678912.csv"
    key = "#{Settings.aws.s3.batch_prefix}/job-#{job_id}/manifest.json"
    json_resp = Minitest::Mock.new
    @mock_s3.expect(:get_object, json_resp, [fixity_bucket, key])
    json_resp.expect(:nil?, false)
    json_body = Minitest::Mock.new
    json_resp.expect(:body, json_body)
    read_resp = { 'Results' => [{ 'Key' => manifest_key }] }.to_json
    json_body.expect(:read, read_resp)

    # parse completion report
    response_target = './report.csv'
    s3_args_verification = [fixity_bucket, manifest_key, response_target]
    @mock_s3.expect(:get_object_to_response_target, [], s3_args_verification)

    table_name = Settings.aws.dynamodb.fixity_table_name
    limit = 1
    key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :s3_key"
    query_resp = Minitest::Mock.new

    # failure item 1
    key1 = '123/test.tst'
    file_id1 = '1'

    # get item id from dynamodb
    expr_attr_vals = { ':s3_key' => key1 }
    items = [{ Settings.aws.dynamodb.file_id => file_id1 }]
    dynamodb_args_ver = [table_name, limit, expr_attr_vals, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp, dynamodb_args_ver)
    query_resp.expect(:nil?, false)
    query_resp.expect(:items, items)
    query_resp.expect(:items, items)

    # failure item 2
    key2 = '123/test1.tst'
    file_id2 = '2'

    # get item id from dynamodb
    expr_attr_vals = { ':s3_key' => key2 }
    items = [{ Settings.aws.dynamodb.file_id => file_id2 }]
    dynamodb_args_ver = [table_name, limit, expr_attr_vals, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp, dynamodb_args_ver)
    query_resp.expect(:nil?, false)
    query_resp.expect(:items, items)
    query_resp.expect(:items, items)

    # check s3 bucket for item to retry restoration
    @mock_s3.expect(:found?, true, [backup_bucket, key2])
    @mock_s3.expect(:restore_object, [], [@mock_dynamodb, backup_bucket, key2, file_id2])
    error_hash2 = {
      Settings.aws.dynamodb.s3_key => key2,
      Settings.aws.dynamodb.file_id => file_id2,
      Settings.aws.dynamodb.err_code => '200',
      Settings.aws.dynamodb.https_status_code => 'PermanentFailure',
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }

    # failure item 2
    key3 = '345/test.tst'
    file_id3 = '3'

    # get item id from dynamodb
    expr_attr_vals = { ':s3_key' => key3 }
    items = [{ Settings.aws.dynamodb.file_id => file_id3 }]
    dynamodb_args_ver = [table_name, limit, expr_attr_vals, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp, dynamodb_args_ver)
    query_resp.expect(:nil?, false)
    query_resp.expect(:items, items)
    query_resp.expect(:items, items)

    # check s3 bucket for item to retry restoration, notify medusa when not found
    @mock_s3.expect(:found?, false, [backup_bucket, key3])
    error_message = "Object with key: #{key3} not found in bucket: #{backup_bucket}"
    @mock_sqs.expect(:send_medusa_message, [], [file_id3, nil, false, Settings.aws.sqs.success, error_message])

    error_hash3 = {
      Settings.aws.dynamodb.s3_key => key3,
      Settings.aws.dynamodb.file_id => file_id3,
      Settings.aws.dynamodb.err_code => '200',
      Settings.aws.dynamodb.https_status_code => Settings.aws.dynamodb.not_found,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    error_batch = [error_hash2, error_hash3]

    # put failures in dynamodb
    put_requests = [[{ put_request: { item: error_hash2 } }, { put_request: { item: error_hash3 } }]]
    @mock_dynamodb.expect(:get_put_requests, put_requests, [error_batch])
    @mock_dynamodb.expect(:batch_write_items, [], [Settings.aws.dynamodb.restoration_errors_table_name, put_requests])

    # remove job id
    key = { Settings.aws.dynamodb.job_id => job_id }
    @mock_dynamodb.expect(:delete_item, [], [key, job_ids_table])

    Time.stub(:now, Time.new(1)) do
      File.stub(:read, File.read("#{ENV['TEST_HOME']}/report.csv")) do
        @p_b_r.process_failures
        assert_mock(@mock_dynamodb)
        assert_mock(@mock_s3_control)
        assert_mock(@mock_s3)
        assert_mock(@mock_sqs)
      end
    end
  end

  def test_process_failures_no_failures
    job_ids_table = Settings.aws.dynamodb.batch_job_ids_table_name

    # get job id
    args_verification = [job_ids_table, 1]
    job_id = 'job-123456789'
    scan_resp = Minitest::Mock.new
    items = [{ Settings.aws.dynamodb.job_id => job_id }]
    scan_resp.expect(:nil?, false)
    scan_resp.expect(:items, items)
    scan_resp.expect(:items, items)
    @mock_dynamodb.expect(:scan, scan_resp, args_verification)

    # get job info
    job_info = Minitest::Mock.new
    @mock_s3_control.expect(:describe_job, job_info, [job_id])
    job_info.expect(:nil?, false)
    job_info.expect(:nil?, false)

    # get job status
    job = Minitest::Mock.new
    job_info.expect(:job, job)
    job.expect(:status, Settings.aws.s3.complete)

    # get duration
    progress_summary = Minitest::Mock.new
    timers = Minitest::Mock.new
    job_info.expect(:job, job)
    job.expect(:progress_summary, progress_summary)
    progress_summary.expect(:timers, timers)
    timers.expect(:elapsed_time_in_active_seconds, 10)
    job_info.expect(:job, job)
    job.expect(:progress_summary, progress_summary)
    progress_summary.expect(:total_number_of_tasks, 100)
    job_info.expect(:job, job)
    job.expect(:job_id, job_id)

    # get tasks failed
    job_info.expect(:job, job)
    job.expect(:progress_summary, progress_summary)
    progress_summary.expect(:number_of_tasks_failed, 0)

    # remove job id
    key = { Settings.aws.dynamodb.job_id => job_id }
    @mock_dynamodb.expect(:delete_item, [], [key, job_ids_table])

    @p_b_r.process_failures
    assert_mock(@mock_dynamodb)
    assert_mock(@mock_s3_control)
  end

  def test_process_failures_job_status_failed
    job_ids_table = Settings.aws.dynamodb.batch_job_ids_table_name

    # get job id
    args_verification = [job_ids_table, 1]
    job_id = 'job-123456789'
    scan_resp = Minitest::Mock.new
    items = [{ Settings.aws.dynamodb.job_id => job_id }]
    scan_resp.expect(:nil?, false)
    scan_resp.expect(:items, items)
    scan_resp.expect(:items, items)
    @mock_dynamodb.expect(:scan, scan_resp, args_verification)

    # get job info
    job_info = Minitest::Mock.new
    @mock_s3_control.expect(:describe_job, job_info, [job_id])
    job_info.expect(:nil?, false)
    job_info.expect(:nil?, false)

    # get job status
    job = Minitest::Mock.new
    job_info.expect(:job, job)
    job.expect(:status, Settings.aws.s3.failed)

    # remove job id
    key = { Settings.aws.dynamodb.job_id => job_id }
    @mock_dynamodb.expect(:delete_item, [], [key, job_ids_table])

    @p_b_r.process_failures
    assert_mock(@mock_dynamodb)
    assert_mock(@mock_s3_control)
  end

  def test_process_failures_job_status_not_complete
    job_ids_table = Settings.aws.dynamodb.batch_job_ids_table_name

    # get job id
    args_verification = [job_ids_table, 1]
    job_id = 'job-123456789'
    scan_resp = Minitest::Mock.new
    items = [{ Settings.aws.dynamodb.job_id => job_id }]
    scan_resp.expect(:nil?, false)
    scan_resp.expect(:items, items)
    scan_resp.expect(:items, items)
    @mock_dynamodb.expect(:scan, scan_resp, args_verification)

    # get job info
    job_info = Minitest::Mock.new
    @mock_s3_control.expect(:describe_job, job_info, [job_id])
    job_info.expect(:nil?, false)
    job_info.expect(:nil?, false)

    # get job status
    job = Minitest::Mock.new
    job_info.expect(:job, job)
    job.expect(:status, 'Active')

    @p_b_r.process_failures
    assert_mock(@mock_dynamodb)
    assert_mock(@mock_s3_control)
  end

  def test_get_job_id_returns_nil
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, 1]
    @mock_dynamodb.expect(:scan, nil, args_verification)
    resp = @p_b_r.get_job_id
    assert_nil(resp)
    assert_mock(@mock_dynamodb)
  end

  def test_get_job_id_returns_job_id
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, 1]
    job_id = 'job-123456789'
    scan_resp = Minitest::Mock.new
    items = [{ Settings.aws.dynamodb.job_id => job_id }]
    scan_resp.expect(:nil?, false)
    scan_resp.expect(:items, items)
    scan_resp.expect(:items, items)
    @mock_dynamodb.expect(:scan, scan_resp, args_verification)
    resp = @p_b_r.get_job_id
    assert_equal(job_id, resp)
    assert_mock(@mock_dynamodb)
  end

  def test_job_info
    mock_describe_job_response = Minitest::Mock.new
    job_id = 'job-123456789'
    @mock_s3_control.expect(:describe_job, mock_describe_job_response, [job_id])
    mock_describe_job_response.expect(:nil?, false)
    @p_b_r.get_job_info(job_id)
    assert_mock(@mock_s3_control)
  end

  def test_job_info_returns_nil
    job_id = 'job-123456789'
    @mock_s3_control.expect(:describe_job, nil, [job_id])
    job_info = @p_b_r.get_job_info(job_id)
    assert_mock(@mock_s3_control)
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
    mock_job.expect(:job_id, 'job-123456')
    @p_b_r.get_duration(mock_job_info)
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
    errors = @p_b_r.get_tasks_failed(mock_job_info)
    assert_mock(mock_job_info)
    assert_mock(mock_job)
    assert_mock(mock_progress_summary)
    assert_equal(1, errors)
  end

  def test_get_job_status
    status_exp = 'Completed'
    mock_job_info = Minitest::Mock.new
    mock_job = Minitest::Mock.new
    mock_job_info.expect(:job, mock_job)
    mock_job.expect(:status, status_exp)
    status_act = @p_b_r.get_job_status(mock_job_info)
    assert_mock(mock_job_info)
    assert_mock(mock_job)
    assert_equal(status_exp, status_act)
  end

  def test_get_manifest_key
    mock_s3_resp = Minitest::Mock.new
    mock_body = Minitest::Mock.new
    test_key = '123/test.tst'
    read_resp = { 'Results' => [{ 'Key' => test_key }] }.to_json
    job_id = 'job-123456789'
    key = "#{Settings.aws.s3.batch_prefix}/job-#{job_id}/manifest.json"
    args_verification = [Settings.aws.s3.fixity_bucket, key]
    @mock_s3.expect(:get_object, mock_s3_resp, args_verification)
    mock_s3_resp.expect(:nil?, false)
    mock_s3_resp.expect(:body, mock_body)
    mock_body.expect(:read, read_resp)
    manifest_key = @p_b_r.get_manifest_key(job_id)
    assert_mock(@mock_s3)
    assert_equal(test_key, manifest_key)
  end

  def test_parse_completion_report
    response_target = './report.csv'
    manifest_key = 'test/test-manifest.csv'
    table_name = Settings.aws.dynamodb.fixity_table_name
    limit = 1
    key1 = '123/test.tst'
    key2 = '123/test1.tst'
    key3 = '345/test.tst'
    expr_attr_vals1 = { ':s3_key' => key1 }
    expr_attr_vals2 = { ':s3_key' => key2 }
    expr_attr_vals3 = { ':s3_key' => key3 }
    key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :s3_key"
    query_resp1 = Object.new
    def query_resp1.items = [{ Settings.aws.dynamodb.file_id => '1' }]
    query_resp2 = Object.new
    def query_resp2.items = [{ Settings.aws.dynamodb.file_id => '2' }]
    query_resp3 = Object.new
    def query_resp3.items = [{ Settings.aws.dynamodb.file_id => '3' }]
    dynamodb_args_ver1 = [table_name, limit, expr_attr_vals1, key_cond_expr]
    dynamodb_args_ver2 = [table_name, limit, expr_attr_vals2, key_cond_expr]
    dynamodb_args_ver3 = [table_name, limit, expr_attr_vals3, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp1, dynamodb_args_ver1)
    @mock_dynamodb.expect(:query, query_resp2, dynamodb_args_ver2)
    @mock_dynamodb.expect(:query, query_resp3, dynamodb_args_ver3)

    s3_args_verification = [Settings.aws.s3.fixity_bucket_arn, manifest_key, response_target]
    @mock_s3.expect(:get_object_to_response_target, [], s3_args_verification)
    error_hash2 = {
      Settings.aws.dynamodb.s3_key => key2,
      Settings.aws.dynamodb.file_id => '2',
      Settings.aws.dynamodb.err_code => '200',
      Settings.aws.dynamodb.https_status_code => 'PermanentFailure',
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    error_hash3 = {
      Settings.aws.dynamodb.s3_key => key3,
      Settings.aws.dynamodb.file_id => '3',
      Settings.aws.dynamodb.err_code => '200',
      Settings.aws.dynamodb.https_status_code => 'PermanentFailure',
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    exp_error_batch = [error_hash2, error_hash3]
    @mock_s3.expect(:found?, true, [Settings.aws.s3.backup_bucket, key2])
    @mock_s3.expect(:found?, true, [Settings.aws.s3.backup_bucket, key3])
    @mock_s3.expect(:restore_object, [], [@mock_dynamodb, Settings.aws.s3.backup_bucket, key2, '2'])
    @mock_s3.expect(:restore_object, [], [@mock_dynamodb, Settings.aws.s3.backup_bucket, key3, '3'])
    Time.stub(:now, Time.new(1)) do
      File.stub(:read, File.read("#{ENV['TEST_HOME']}/report.csv")) do
        error_batch = @p_b_r.parse_completion_report(manifest_key)
        assert_equal(exp_error_batch, error_batch)
      end
    end
    assert_mock(@mock_s3)
  end

  def test_get_file_id
    table_name = Settings.aws.dynamodb.fixity_table_name
    query_resp = Object.new
    def query_resp.items = [{ Settings.aws.dynamodb.file_id => '123' }]
    limit = 1
    expr_attr_vals = { ':s3_key' => '123/test.tst' }
    key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :s3_key"
    args_verification = [table_name, limit, expr_attr_vals, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp, args_verification)
    file_id = @p_b_r.get_file_id('123/test.tst')
    assert_mock(@mock_dynamodb)
    assert_equal('123', file_id)
  end

  def test_remove_job_id
    job_id = 'job-123456789'
    key = { Settings.aws.dynamodb.job_id => job_id }
    args_verification = [key, Settings.aws.dynamodb.batch_job_ids_table_name]
    @mock_dynamodb.expect(:delete_item, [], args_verification)
    @p_b_r.remove_job_id(job_id)
    assert_mock(@mock_dynamodb)
  end
end
