require 'minitest/autorun'
require 'config'
require 'csv'
require 'json'

require_relative '../lib/batch_restore_files'

class TestBatchRestoreFiles < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", 'test'))

  def setup
    @mock_s3 = Minitest::Mock.new
    @mock_dynamodb = Minitest::Mock.new
    @mock_s3_control = Minitest::Mock.new
    @mock_db = Minitest::Mock.new
    @batch_restore_files = BatchRestoreFiles.new(@mock_s3, @mock_dynamodb, @mock_s3_control, @mock_db)
  end

  def test_batch_restore
    medusa_db_id_table = Settings.aws.dynamodb.medusa_db_id_table_name
    manifest = 'manifest-0002-01-01-00:00.csv'
    etag = '98765432109876543210987654321021'

    # get medusa id
    id = '1'
    expr_attr_vals = { ':file_type' => Settings.aws.dynamodb.current_id, }
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :file_type"
    args_verification = [medusa_db_id_table, 1, expr_attr_vals, key_cond_expr]
    mock_query_resp = Minitest::Mock.new
    items = [{Settings.aws.dynamodb.file_id => id}]
    mock_query_resp.expect(:nil?, false)
    # return items when checking items empty?
    mock_query_resp.expect(:items, items)
    mock_query_resp.expect(:items, items)
    @mock_dynamodb.expect(:query, mock_query_resp, args_verification)

    # get max id
    id_iterator = '2'
    sql = 'SELECT MAX(id) FROM cfs_files'
    @mock_db.expect(:exec, [{'max' => id_iterator}], [sql])

    # get files in batches
    sql = 'SELECT * FROM cfs_files WHERE id>$1 AND  id<=$2'
    files_ret = [{'id' => '1', 'name' => 'test', 'size' => 123456, 'md5_sum' => '12345678901234567890123456789012', 'cfs_directory_id' => '3'},
                 {'id' => '2', 'name' => 'test1', 'size' => 234567, 'md5_sum' => '23456789012345678901234567890123', 'cfs_directory_id' => '6'}
    ]
    @mock_db.expect(:exec_params, files_ret, [sql, [{:value =>id.to_s}, {:value =>id_iterator.to_s}]])

    # get path hash
    sql = 'SELECT * FROM cfs_directories WHERE id=$1'
    row1 = [{'path' => '6', 'parent_id' => '5', 'parent_type' => 'CfsDirectory'}]
    row2 = [{'path' => '5', 'parent_id' => '4', 'parent_type' => 'CfsDirectory'}]
    row3 = [{'path' => '4', 'parent_id' => '3', 'parent_type' => 'FileGroup'}]
    row4 = [{'path' => '3', 'parent_id' => '2', 'parent_type' => 'CfsDirectory'}]
    row5 = [{'path' => '2', 'parent_id' => '1', 'parent_type' => 'CfsDirectory'}]
    row6 = [{'path' => '1', 'parent_id' => nil, 'parent_type' => nil}]
    @mock_db.expect(:exec_params, row4, [sql, [{:value =>'3'}]])
    @mock_db.expect(:exec_params, row5, [sql, [{:value =>'2'}]])
    @mock_db.expect(:exec_params, row6, [sql, [{:value =>'1'}]])
    @mock_db.expect(:exec_params, row1, [sql, [{:value =>'6'}]])
    @mock_db.expect(:exec_params, row2, [sql, [{:value =>'5'}]])
    @mock_db.expect(:exec_params, row3, [sql, [{:value =>'4'}]])

    # generate manifest

    # get_put_requests
    test_batch_item1 = {
      Settings.aws.dynamodb.s3_key => '1/2/3/test',
      Settings.aws.dynamodb.file_id => '1',
      Settings.aws.dynamodb.initial_checksum => '12345678901234567890123456789012',
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3)
    }
    test_batch_item2 = {
      Settings.aws.dynamodb.s3_key => '4/5/6/test1',
      Settings.aws.dynamodb.file_id => '2',
      Settings.aws.dynamodb.initial_checksum => '23456789012345678901234567890123',
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3)
    }
    test_batch = [test_batch_item1, test_batch_item2]

    put_request = [{ put_request: { item: test_batch_item1 } }, { put_request: { item: test_batch_item2 } }]
    @mock_dynamodb.expect(:get_put_requests, put_request, [test_batch])

    # batch write item
    @mock_dynamodb.expect(:batch_write_items, [], [Settings.aws.dynamodb.fixity_table_name, put_request])

    # put medusa id
    item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_id,
      Settings.aws.dynamodb.file_id => '2'.to_s
    }
    args_verification = [medusa_db_id_table, item]
    @mock_dynamodb.expect(:put_item, [], args_verification)

    # put manifest
    mock_s3_resp = Minitest::Mock.new
    mock_s3_resp.expect(:etag, etag)
    @mock_s3.expect(:put_object, mock_s3_resp, [File, Settings.aws.s3.fixity_bucket, "fixity/#{manifest}"])

    # send batch job
    # get request token
    limit = 1
    expr_attr_values = { ':request_token' => Settings.aws.dynamodb.current_request_token,}
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :request_token"
    request_toke_exp = 123
    query_resp = Minitest::Mock.new
    query_resp.expect(:items, [{Settings.aws.dynamodb.file_id => request_toke_exp.to_s}])
    query_resp.expect(:nil?, false)
    query_resp.expect(:items, [{Settings.aws.dynamodb.file_id => request_toke_exp.to_s}])
    args_validation = [medusa_db_id_table, limit, expr_attr_values, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp, args_validation)

    job_id = 'job-123456789'
    mock_resp = Minitest::Mock.new
    mock_resp.expect(:job_id, job_id)
    @mock_s3_control.expect(:create_job, mock_resp, [manifest, request_toke_exp+1, etag])

    # put job id
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, { Settings.aws.dynamodb.job_id => job_id }]
    @mock_dynamodb.expect(:put_item, [], args_verification)

    # put request token
    args_verification = [medusa_db_id_table, { Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_request_token,
                                               Settings.aws.dynamodb.file_id => (request_toke_exp+1).to_s}]
    @mock_dynamodb.expect(:put_item, [], args_verification)

    # run test
    Time.stub(:now, Time.new(2)) do
      @batch_restore_files.batch_restore
      assert_mock(@mock_dynamodb)
      assert_mock(@mock_db)
      assert_mock(@mock_s3)
      assert_mock(@mock_s3_control)
    end
  end

  def test_batch_restore_empty_batch
    medusa_db_id_table = Settings.aws.dynamodb.medusa_db_id_table_name

    # get medusa id
    id = '1'
    expr_attr_vals = { ':file_type' => Settings.aws.dynamodb.current_id, }
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :file_type"
    args_verification = [medusa_db_id_table, 1, expr_attr_vals, key_cond_expr]
    mock_query_resp = Minitest::Mock.new
    items = [{Settings.aws.dynamodb.file_id => id}]
    mock_query_resp.expect(:nil?, false)
    # return items when checking items empty?
    mock_query_resp.expect(:items, items)
    mock_query_resp.expect(:items, items)
    @mock_dynamodb.expect(:query, mock_query_resp, args_verification)

    # get max id
    id_iterator = '2'
    sql = 'SELECT MAX(id) FROM cfs_files'
    @mock_db.expect(:exec, [{'max' => id_iterator}], [sql])

    # get files in batches
    sql = 'SELECT * FROM cfs_files WHERE id>$1 AND  id<=$2'
    files_ret = []
    @mock_db.expect(:exec_params, files_ret, [sql, [{:value =>id.to_s}, {:value =>id_iterator.to_s}]])

    # get path hash

    # generate manifest

    # get_put_requests
    @mock_dynamodb.expect(:get_put_requests, [], [[]])

    # batch write item
    @mock_dynamodb.expect(:batch_write_items, [], [Settings.aws.dynamodb.fixity_table_name, []])

    # put medusa id
    item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_id,
      Settings.aws.dynamodb.file_id => '2'.to_s
    }
    args_verification = [medusa_db_id_table, item]
    @mock_dynamodb.expect(:put_item, [], args_verification)

    # run test
    Time.stub(:now, Time.new(2)) do
      @batch_restore_files.batch_restore
      assert_mock(@mock_dynamodb)
      assert_mock(@mock_db)
      assert_mock(@mock_s3)
      assert_mock(@mock_s3_control)
    end
  end

  def test_batch_restore_done
    medusa_db_id_table = Settings.aws.dynamodb.medusa_db_id_table_name

    # get medusa id
    id = '123456789'
    expr_attr_vals = { ':file_type' => Settings.aws.dynamodb.current_id, }
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :file_type"
    args_verification = [medusa_db_id_table, 1, expr_attr_vals, key_cond_expr]
    mock_query_resp = Minitest::Mock.new
    items = [{Settings.aws.dynamodb.file_id => id}]
    mock_query_resp.expect(:nil?, false)
    # return items when checking items empty?
    mock_query_resp.expect(:items, items)
    mock_query_resp.expect(:items, items)
    @mock_dynamodb.expect(:query, mock_query_resp, args_verification)

    # get max id
    id_iterator = '123456789'
    sql = 'SELECT MAX(id) FROM cfs_files'
    @mock_db.expect(:exec, [{'max' => id_iterator}], [sql])

    @batch_restore_files.batch_restore
    assert_mock(@mock_dynamodb)
    assert_mock(@mock_db)
  end

  def test_get_batch_restore_from_list
    # get batch from list
    # list item 1
    id1 = 1
    sql = 'SELECT * FROM cfs_files WHERE id=$1'
    files_ret = [{'id' => id1.to_s, 'name' => 'test', 'size' => 123456, 'md5_sum' => '12345678901234567890123456789012', 'cfs_directory_id' => '3'}]
    @mock_db.expect(:exec_params, files_ret, [sql, [value: id1.to_s]])

    id2 = 2
    sql = 'SELECT * FROM cfs_files WHERE id=$1'
    files_ret = [{'id' => id2.to_s, 'name' => 'test1', 'size' => 234567, 'md5_sum' => '23456789012345678901234567890123', 'cfs_directory_id' => '6'}]
    @mock_db.expect(:exec_params, files_ret, [sql, [value: id2.to_s]])

    # get path hash
    sql = 'SELECT * FROM cfs_directories WHERE id=$1'
    row1 = [{'path' => '6', 'parent_id' => '5', 'parent_type' => 'CfsDirectory'}]
    row2 = [{'path' => '5', 'parent_id' => '4', 'parent_type' => 'CfsDirectory'}]
    row3 = [{'path' => '4', 'parent_id' => '3', 'parent_type' => 'FileGroup'}]
    row4 = [{'path' => '3', 'parent_id' => '2', 'parent_type' => 'CfsDirectory'}]
    row5 = [{'path' => '2', 'parent_id' => '1', 'parent_type' => 'CfsDirectory'}]
    row6 = [{'path' => '1', 'parent_id' => nil, 'parent_type' => nil}]
    @mock_db.expect(:exec_params, row4, [sql, [{:value =>'3'}]])
    @mock_db.expect(:exec_params, row5, [sql, [{:value =>'2'}]])
    @mock_db.expect(:exec_params, row6, [sql, [{:value =>'1'}]])
    @mock_db.expect(:exec_params, row1, [sql, [{:value =>'6'}]])
    @mock_db.expect(:exec_params, row2, [sql, [{:value =>'5'}]])
    @mock_db.expect(:exec_params, row3, [sql, [{:value =>'4'}]])

    # generate manifest
    # get put requests
    test_batch_item1 = {
      Settings.aws.dynamodb.s3_key => '1/2/3/test',
      Settings.aws.dynamodb.file_id => '1',
      Settings.aws.dynamodb.initial_checksum => '12345678901234567890123456789012',
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3)
    }
    test_batch_item2 = {
      Settings.aws.dynamodb.s3_key => '4/5/6/test1',
      Settings.aws.dynamodb.file_id => '2',
      Settings.aws.dynamodb.initial_checksum => '23456789012345678901234567890123',
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(2).getutc.iso8601(3)
    }
    test_batch = [test_batch_item1, test_batch_item2]

    put_request = [{ put_request: { item: test_batch_item1 } }, { put_request: { item: test_batch_item2 } }]
    @mock_dynamodb.expect(:get_put_requests, put_request, [test_batch])

    # batch write item
    @mock_dynamodb.expect(:batch_write_items, [], [Settings.aws.dynamodb.fixity_table_name, put_request])

    # put manifest
    manifest = 'manifest-0002-01-01-00:00.csv'
    etag = '98765432109876543210987654321021'
    mock_s3_resp = Minitest::Mock.new
    mock_s3_resp.expect(:etag, etag)
    @mock_s3.expect(:put_object, mock_s3_resp, [File, Settings.aws.s3.fixity_bucket, "fixity/#{manifest}"])

    # send job
    medusa_db_id_table = Settings.aws.dynamodb.medusa_db_id_table_name
    limit = 1
    expr_attr_values = { ':request_token' => Settings.aws.dynamodb.current_request_token,}
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :request_token"
    request_toke_exp = 123
    query_resp = Minitest::Mock.new
    query_resp.expect(:items, [{Settings.aws.dynamodb.file_id => request_toke_exp.to_s}])
    query_resp.expect(:nil?, false)
    query_resp.expect(:items, [{Settings.aws.dynamodb.file_id => request_toke_exp.to_s}])
    args_validation = [medusa_db_id_table, limit, expr_attr_values, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp, args_validation)

    job_id = 'job-123456789'
    mock_resp = Minitest::Mock.new
    mock_resp.expect(:job_id, job_id)
    @mock_s3_control.expect(:create_job, mock_resp, [manifest, request_toke_exp+1, etag])

    # put job id
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, { Settings.aws.dynamodb.job_id => job_id }]
    @mock_dynamodb.expect(:put_item, [], args_verification)

    # put request token
    args_verification = [medusa_db_id_table, { Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_request_token,
                                               Settings.aws.dynamodb.file_id => (request_toke_exp+1).to_s}]
    @mock_dynamodb.expect(:put_item, [], args_verification)
    Time.stub(:now, Time.new(2)) do
      @batch_restore_files.batch_restore_from_list([1, 2])
      assert_mock(@mock_dynamodb)
      assert_mock(@mock_db)
      assert_mock(@mock_s3)
      assert_mock(@mock_s3_control)
    end
  end

  def test_get_medusa_id
    expr_attr_vals = { ':file_type' => Settings.aws.dynamodb.current_id, }
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :file_type"
    args_verification = [Settings.aws.dynamodb.medusa_db_id_table_name, 1, expr_attr_vals, key_cond_expr]
    mock_query_resp = Minitest::Mock.new
    items = [{Settings.aws.dynamodb.file_id => '12345689'}]
    mock_query_resp.expect(:nil?, false)
    # return items when checking items empty?
    mock_query_resp.expect(:items, items)
    mock_query_resp.expect(:items, items)
    @mock_dynamodb.expect(:query, mock_query_resp, args_verification)
    medusa_id = @batch_restore_files.get_medusa_id
    assert_mock(@mock_dynamodb)
    assert_equal(12_345_689, medusa_id)
  end

  def test_get_max_id
    sql = 'SELECT MAX(id) FROM cfs_files'
    @mock_db.expect(:exec, [{'max' => '123456789123'}], [sql])
    max_id = @batch_restore_files.get_max_id
    assert_mock(@mock_db)
    assert_equal(123456789123, max_id)
  end

  def test_evaluate_done
    assert_equal(true, @batch_restore_files.evaluate_done(1, 0))
    assert_equal(true, @batch_restore_files.evaluate_done(1, 1))
    assert_equal(false, @batch_restore_files.evaluate_done(0, 1))
  end

  def test_get_id_iterator
    max_batch_count = @batch_restore_files.max_batch_count

    # test when id + 1000 less than max id and less than remaining batch count
    batch_count = max_batch_count - 5000
    id = (batch_count - 1000) - 50
    max_id = id + 1000 + 5
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(true, continue)
    assert_equal(id + 1000, id_itr)

    # test when id + 1000 equal to max id and less than remaining batch count
    batch_count = max_batch_count - 5000
    id = batch_count - 1000 - 50
    max_id = id + 1000
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(id + 1000, id_itr)

    # test when id + 1000 greater than max id and less than remaining batch count
    batch_count = max_batch_count - 5000
    id = batch_count - 1000 - 50
    max_id = id + 900
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(max_id, id_itr)

    # test when id + 1000 less than max id and greater than remaining batch count
    batch_count = max_batch_count - 950
    id = batch_count
    max_id = id + 2000
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(max_batch_count, id_itr)

    # test when id + 1000 less than max id and equal to remaining batch count
    batch_count = max_batch_count - 1000
    id = batch_count
    max_id = id + 2000
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(max_batch_count, id_itr)

    # test when id + 1000 equal to max id and greater than remaining batch count
    batch_count = max_batch_count - 500
    id = batch_count
    max_id = id + 1000
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(max_id, id_itr)

    # test when id + 1000 greater than max id and greater than remaining batch count
    batch_count = max_batch_count - 300
    id = batch_count
    max_id = id + 500
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(max_id, id_itr)

    # test when id + 1000 greater than max id and equal to remaining batch count
    batch_count = max_batch_count - 1000
    id = batch_count
    max_id = id + 500
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(max_id, id_itr)

    # test when id + 1000 equal to max id and equal to remaining batch count
    batch_count = max_batch_count - 1000
    id = batch_count
    max_id = id + 1000
    id_itr, continue = @batch_restore_files.get_id_iterator(id, max_id, batch_count)
    assert_equal(false, continue)
    assert_equal(max_id, id_itr)
  end

  def test_get_file
    id = 123
    sql = 'SELECT * FROM cfs_files WHERE id=$1'
    file_exp = {'id' => '123', 'name' => 'test', 'size' => 123456, 'md5_sum' => '12345678901234567890123456789012', 'cfs_directory_id' => 1}
    @mock_db.expect(:exec_params, [file_exp], [sql, [{:value =>id.to_s}]])
    file_act = @batch_restore_files.get_file(id)
    assert_mock(@mock_db)
    assert_equal(file_exp, file_act)
  end

  def test_get_files_in_batches
    id = 0
    id_iterator = 3
    sql = 'SELECT * FROM cfs_files WHERE id>$1 AND  id<=$2'
    files_ret = [{'id' => '1', 'name' => 'test', 'size' => 123456, 'md5_sum' => '12345678901234567890123456789012', 'cfs_directory_id' => 1},
                 {'id' => '2', 'name' => 'test1', 'size' => 234567, 'md5_sum' => '23456789012345678901234567890123', 'cfs_directory_id' => 1},
                 {'id' => '3', 'name' => 'test2', 'size' => 345678, 'md5_sum' => '34567890123456789012345678901234', 'cfs_directory_id' => 2}
    ]
    @mock_db.expect(:exec_params, files_ret, [sql, [{:value =>id.to_s}, {:value =>id_iterator.to_s}]])
    file_dirs_act, medusa_files_act, size_act = @batch_restore_files.get_files_in_batches(id, id_iterator)

    m_file_1 = MedusaFile.new('test', '1', 1, '12345678901234567890123456789012')
    m_file_2 = MedusaFile.new('test1', '2', 1, '23456789012345678901234567890123')
    m_file_3 = MedusaFile.new('test2', '3', 2, '34567890123456789012345678901234')
    medusa_files_exp = [m_file_1, m_file_2, m_file_3]
    file_dirs_exp = [1,2]
    size_exp = 123456 + 234567 + 345678

    assert_mock(@mock_db)
    assert_equal(file_dirs_exp, file_dirs_act)
    assert_equal(medusa_files_exp, medusa_files_act)
    assert_equal(size_exp, size_act)
  end

  def test_get_path
    row1 = [{'path' => '3', 'parent_id' => 2, 'parent_type' => 'CfsDirectory'}]
    row2 = [{'path' => '2', 'parent_id' => 1, 'parent_type' => 'CfsDirectory'}]
    row3 = [{'path' => '1', 'parent_id' => nil, 'parent_type' => nil}]
    sql = 'SELECT * FROM cfs_directories WHERE id=$1'
    path = 'test.tst'
    path_exp = '1/2/3/'+path
    @mock_db.expect(:exec_params, row1, [sql, [{:value =>3}]])
    @mock_db.expect(:exec_params, row2, [sql, [{:value =>2}]])
    @mock_db.expect(:exec_params, row3, [sql, [{:value =>1}]])
    path_act = @batch_restore_files.get_path(3, path)
    assert_equal(path_exp, path_act)
    assert_mock(@mock_db)
  end

  def test_get_path_escape_special_characters
    row1 = [{'path' => '3', 'parent_id' => 2, 'parent_type' => 'CfsDirectory'}]
    row2 = [{'path' => '2', 'parent_id' => 1, 'parent_type' => 'CfsDirectory'}]
    row3 = [{'path' => '1', 'parent_id' => nil, 'parent_type' => nil}]
    sql = 'SELECT * FROM cfs_directories WHERE id=$1'
    path = '&test.tst'
    path_exp = '1/2/3/%26test.tst'
    @mock_db.expect(:exec_params, row1, [sql, [{:value =>3}]])
    @mock_db.expect(:exec_params, row2, [sql, [{:value =>2}]])
    @mock_db.expect(:exec_params, row3, [sql, [{:value =>1}]])
    path_act = @batch_restore_files.get_path(3, path)
    assert_equal(path_exp, path_act)
    assert_mock(@mock_db)
  end

  def test_get_path_not_cfs_dir
    row1 = [{'path' => '6', 'parent_id' => 5, 'parent_type' => 'CfsDirectory'}]
    row2 = [{'path' => '5', 'parent_id' => 4, 'parent_type' => 'CfsDirectory'}]
    row3 = [{'path' => '4', 'parent_id' => 3, 'parent_type' => 'FileGroup'}]
    sql = 'SELECT * FROM cfs_directories WHERE id=$1'
    path = 'test.tst'
    path_exp = '4/5/6/'+path
    @mock_db.expect(:exec_params, row1, [sql, [{:value =>6}]])
    @mock_db.expect(:exec_params, row2, [sql, [{:value =>5}]])
    @mock_db.expect(:exec_params, row3, [sql, [{:value =>4}]])
    path_act = @batch_restore_files.get_path(6, path)
    assert_equal(path_exp, path_act)
    assert_mock(@mock_db)
  end

  def test_get_path_hash
    file_directories = [6, 3]
    dir_exp = {6 => '4/5/6/', 3 => '1/2/3/'}
    sql = 'SELECT * FROM cfs_directories WHERE id=$1'
    row1 = [{'path' => '6', 'parent_id' => 5, 'parent_type' => 'CfsDirectory'}]
    row2 = [{'path' => '5', 'parent_id' => 4, 'parent_type' => 'CfsDirectory'}]
    row3 = [{'path' => '4', 'parent_id' => 3, 'parent_type' => 'FileGroup'}]
    row4 = [{'path' => '3', 'parent_id' => 2, 'parent_type' => 'CfsDirectory'}]
    row5 = [{'path' => '2', 'parent_id' => 1, 'parent_type' => 'CfsDirectory'}]
    row6 = [{'path' => '1', 'parent_id' => nil, 'parent_type' => nil}]
    @mock_db.expect(:exec_params, row1, [sql, [{:value =>6}]])
    @mock_db.expect(:exec_params, row2, [sql, [{:value =>5}]])
    @mock_db.expect(:exec_params, row3, [sql, [{:value =>4}]])
    @mock_db.expect(:exec_params, row4, [sql, [{:value =>3}]])
    @mock_db.expect(:exec_params, row5, [sql, [{:value =>2}]])
    @mock_db.expect(:exec_params, row6, [sql, [{:value =>1}]])
    dir_act = @batch_restore_files.get_path_hash(file_directories)
    assert_equal(dir_exp, dir_act)
    assert_mock(@mock_db)
  end

  def test_generate_manifest
    manifest = 'test-manifest.csv'
    dir_path_1 = '1/2/3/'
    dir_path_2 = '4/5/6/'
    directories = {'1' => dir_path_1, '2' => dir_path_2}
    id_1 = '123'
    dir_1 = 1
    name_1 = 'test'
    checksum_1 = '12345678901234567890123456789012'
    key_1 = dir_path_1 + name_1
    id_2 = '345'
    dir_2 = 2
    name_2 = 'test1'
    checksum_2 = '23456789012345678901234567890123'
    key_2 = dir_path_2 + name_2
    id_3 = '456'
    dir_3 = 1
    name_3 = 'test3'
    key_3 = dir_path_1 + name_3
    checksum_3 = '34567890123456789012345678901234'
    medusa_item_1 = MedusaFile.new(name_1, id_1, dir_1, checksum_1)
    medusa_item_2 = MedusaFile.new(name_2, id_2, dir_2, checksum_2)
    medusa_item_3 = MedusaFile.new(name_3, id_3, dir_3, checksum_3)
    medusa_files = [medusa_item_1, medusa_item_2, medusa_item_3]
    batch_hash_1 = {
      Settings.aws.dynamodb.s3_key => key_1,
      Settings.aws.dynamodb.file_id => id_1,
      Settings.aws.dynamodb.initial_checksum => checksum_1,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    batch_hash_2 = {
      Settings.aws.dynamodb.s3_key => key_2,
      Settings.aws.dynamodb.file_id => id_2,
      Settings.aws.dynamodb.initial_checksum => checksum_2,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    batch_hash_3 = {
      Settings.aws.dynamodb.s3_key => key_3,
      Settings.aws.dynamodb.file_id => id_3,
      Settings.aws.dynamodb.initial_checksum => checksum_3,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    batch_exp = [batch_hash_1, batch_hash_2, batch_hash_3]
    keys = [key_1, key_2, key_3]
    Time.stub(:now, Time.new(1)) do
      batch_act = @batch_restore_files.generate_manifest(manifest, medusa_files, directories)
      manifest_table = CSV.new(File.read(manifest))
      manifest_table.each do |row|
        bucket, key = row
        assert_equal(bucket, Settings.aws.s3.backup_bucket)
        assert_equal(true, keys.include?(key))
      end
      assert_equal(batch_exp, batch_act)
    end
    File.truncate(manifest, 0)
  end

  def test_generate_manifest_escapes_special_characters
    manifest = 'test-special-manifest.csv'
    dir_path_1 = '1/2/3/'
    dir_path_2 = '4/5/6/'
    directories = {'1' => dir_path_1, '2' => dir_path_2}
    id_1 = '123'
    dir_1 = 1
    name_1 = '#test'
    name_1_esc = '%23test'
    checksum_1 = '12345678901234567890123456789012'
    key_1 = dir_path_1 + name_1_esc
    id_2 = '345'
    dir_2 = 2
    name_2 = '@test1'
    name_2_esc = '%40test1'
    checksum_2 = '23456789012345678901234567890123'
    key_2 = dir_path_2 + name_2_esc
    id_3 = '456'
    dir_3 = 1
    name_3 = '=test3'
    name_3_esc = '%3Dtest3'
    key_3 = dir_path_1 + name_3_esc
    checksum_3 = '34567890123456789012345678901234'
    medusa_item_1 = MedusaFile.new(name_1, id_1, dir_1, checksum_1)
    medusa_item_2 = MedusaFile.new(name_2, id_2, dir_2, checksum_2)
    medusa_item_3 = MedusaFile.new(name_3, id_3, dir_3, checksum_3)
    medusa_files = [medusa_item_1, medusa_item_2, medusa_item_3]
    batch_hash_1 = {
      Settings.aws.dynamodb.s3_key => key_1,
      Settings.aws.dynamodb.file_id => id_1,
      Settings.aws.dynamodb.initial_checksum => checksum_1,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    batch_hash_2 = {
      Settings.aws.dynamodb.s3_key => key_2,
      Settings.aws.dynamodb.file_id => id_2,
      Settings.aws.dynamodb.initial_checksum => checksum_2,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    batch_hash_3 = {
      Settings.aws.dynamodb.s3_key => key_3,
      Settings.aws.dynamodb.file_id => id_3,
      Settings.aws.dynamodb.initial_checksum => checksum_3,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    batch_exp = [batch_hash_1, batch_hash_2, batch_hash_3]
    keys = [key_1, key_2, key_3]
    Time.stub(:now, Time.new(1)) do
      batch_act = @batch_restore_files.generate_manifest(manifest, medusa_files, directories)
      manifest_table = CSV.new(File.read(manifest))
      manifest_table.each do |row|
        bucket, key = row
        assert_equal(bucket, Settings.aws.s3.backup_bucket)
        assert_equal(true, keys.include?(key))
      end
      assert_equal(batch_exp, batch_act)
    end
    File.truncate(manifest, 0)
  end

  def test_put_medusa_id
    id = '123'
    test_item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_id,
      Settings.aws.dynamodb.file_id => id.to_s,
    }
    args_verification = [Settings.aws.dynamodb.medusa_db_id_table_name, test_item]
    @mock_dynamodb.expect(:put_item, [], args_verification)
    @batch_restore_files.put_medusa_id(id)
    assert_mock(@mock_dynamodb)
  end

  def test_put_batch_item
    id = '123'
    key = '123/test.tst'
    checksum = '12345678901234567890123456789012'
    batch_item = BatchItem.new(key, id, checksum)
    test_item = {
      Settings.aws.dynamodb.s3_key => batch_item.s3_key,
      Settings.aws.dynamodb.file_id => batch_item.file_id,
      Settings.aws.dynamodb.initial_checksum => batch_item.initial_checksum,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    args_verification = [Settings.aws.dynamodb.fixity_table_name, test_item]
    @mock_dynamodb.expect(:put_item, [], args_verification)
    Time.stub(:now, Time.new(1)) do
      @batch_restore_files.put_batch_item(batch_item)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_put_manifest
    mock_file = Minitest::Mock.new
    manifest = 'test-manifest.csv'
    key = "fixity/#{manifest}"
    s3_resp = Object.new
    def s3_resp.etag = '98765432109876543210987654321021'
    args_verification = [Minitest::Mock, Settings.aws.s3.fixity_bucket, key]
    @mock_s3.expect(:put_object, s3_resp, args_verification)
    File.stub(:new, mock_file) do
      etag_act = @batch_restore_files.put_manifest(manifest)
      assert_mock(@mock_s3)
      assert_equal('98765432109876543210987654321021', etag_act)
    end
  end

  def test_get_batch_from_list
    id_1 = '123'
    dir_1 = 1
    name_1 = 'test'
    size_1 = 1234
    checksum_1 = '12345678901234567890123456789012'
    id_2 = '345'
    dir_2 = 2
    name_2 = 'test1'
    size_2 = 2345
    checksum_2 = '23456789012345678901234567890123'
    id_3 = '456'
    dir_3 = 1
    name_3 = 'test3'
    size_3 = 3456
    checksum_3 = '34567890123456789012345678901234'
    list = [id_1, id_2, id_3]
    ret_val_1 = [{'id' => id_1, 'cfs_directory_id' => dir_1, 'name' => name_1, 'size' => size_1, 'md5_sum' => checksum_1}]
    sql = 'SELECT * FROM cfs_files WHERE id=$1'
    @mock_db.expect(:exec_params, ret_val_1, [sql, [{:value =>id_1.to_s}]])
    ret_val_2 = [{'id' => id_2, 'cfs_directory_id' => dir_2, 'name' => name_2, 'size' => size_2, 'md5_sum' => checksum_2}]
    @mock_db.expect(:exec_params, ret_val_2, [sql, [{:value =>id_2.to_s}]])
    ret_val_3 = [{'id' => id_3, 'cfs_directory_id' => dir_3, 'name' => name_3, 'size' => size_3, 'md5_sum' => checksum_3}]
    @mock_db.expect(:exec_params, ret_val_3, [sql, [{:value =>id_3.to_s}]])
    medusa_item_1 = MedusaFile.new(name_1, id_1, dir_1, checksum_1)
    medusa_item_2 = MedusaFile.new(name_2, id_2, dir_2, checksum_2)
    medusa_item_3 = MedusaFile.new(name_3, id_3, dir_3, checksum_3)
    medusa_files_exp = [medusa_item_1, medusa_item_2, medusa_item_3]
    file_dirs_exp = [1, 2]
    size_exp = size_1 + size_2 + size_3

    file_dirs_act, medusa_files_act, size_act = @batch_restore_files.get_batch_from_list(list)
    assert_mock(@mock_db)
    assert_equal(file_dirs_exp, file_dirs_act)
    assert_equal(medusa_files_exp, medusa_files_act)
    assert_equal(size_exp, size_act)
  end

  def test_restore_item
    id = '123'
    key = '123/test.tst'
    checksum = '12345678901234567890123456789012'
    batch_item = BatchItem.new(key, id, checksum)
    @mock_s3.expect(:restore_object, [], [@mock_dynamodb, Settings.aws.s3.backup_bucket, key, id] )
    test_item = {
      Settings.aws.dynamodb.s3_key => batch_item.s3_key,
      Settings.aws.dynamodb.file_id => batch_item.file_id,
      Settings.aws.dynamodb.initial_checksum => batch_item.initial_checksum,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    @mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.fixity_table_name, test_item])
    Time.stub(:now, Time.new(1)) do
      @batch_restore_files.restore_item(batch_item)
      assert_mock(@mock_s3)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_restore_item_special_characters
    id = '123'
    key_unesc = '123/%24test.tst'
    key = '123/$test.tst'
    checksum = '12345678901234567890123456789012'
    batch_item = BatchItem.new(key_unesc, id, checksum)
    @mock_s3.expect(:restore_object, [], [@mock_dynamodb, Settings.aws.s3.backup_bucket, key, id] )
    test_item = {
      Settings.aws.dynamodb.s3_key => batch_item.s3_key,
      Settings.aws.dynamodb.file_id => batch_item.file_id,
      Settings.aws.dynamodb.initial_checksum => batch_item.initial_checksum,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    @mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.fixity_table_name, test_item])
    Time.stub(:now, Time.new(1)) do
      @batch_restore_files.restore_item(batch_item)
      assert_mock(@mock_s3)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_send_batch_job
    manifest = 'test-manifest.csv'
    etag = '98765432109876543210987654321021'
    job_id = 'job-123456789'
    token = 2
    @batch_restore_files.stub(:get_request_token, token) do
      mock_resp = Minitest::Mock.new
      mock_resp.expect(:job_id, job_id)
      @mock_s3_control.expect(:create_job,  mock_resp, [manifest, token+1, etag])
      @mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.batch_job_ids_table_name,
                                                            {Settings.aws.dynamodb.job_id => job_id, }])
      @mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.medusa_db_id_table_name,
                                                          {Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_request_token,
                                                          Settings.aws.dynamodb.file_id => (token+1).to_s,}])
      @batch_restore_files.send_batch_job(manifest, etag)
      assert_mock(@mock_s3_control)
      assert_mock(@mock_dynamodb)
    end
  end

  def test_get_request_token
    table_name = Settings.aws.dynamodb.medusa_db_id_table_name
    limit = 1
    expr_attr_values = { ':request_token' => Settings.aws.dynamodb.current_request_token,}
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :request_token"
    request_toke_exp = 123
    query_resp = Minitest::Mock.new
    query_resp.expect(:items, [{Settings.aws.dynamodb.file_id => request_toke_exp.to_s}])
    query_resp.expect(:nil?, false)
    query_resp.expect(:items, [{Settings.aws.dynamodb.file_id => request_toke_exp.to_s}])
    args_validation = [table_name, limit, expr_attr_values, key_cond_expr]
    @mock_dynamodb.expect(:query, query_resp, args_validation)
    request_token_act = @batch_restore_files.get_request_token
    assert_mock(@mock_dynamodb)
    assert_equal(request_toke_exp, request_token_act)
  end

  def test_put_request_token
    token = '1'
    item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_request_token,
      Settings.aws.dynamodb.file_id => token.to_s,
    }
    args_verification = [Settings.aws.dynamodb.medusa_db_id_table_name, item]
    @mock_dynamodb.expect(:put_item, [], args_verification)
    @batch_restore_files.put_request_token(token)
    assert_mock(@mock_dynamodb)
  end

  def test_put_job_id
    job_id = 'job-123456789'
    item = { Settings.aws.dynamodb.job_id => job_id, }
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, item]
    @mock_dynamodb.expect(:put_item, [], args_verification)
    @batch_restore_files.put_job_id(job_id)
    assert_mock(@mock_dynamodb)
  end
end
