require 'minitest/autorun'
require 'config'
require 'json'

require_relative '../lib/batch_restore_files'

class TestBatchRestoreFiles < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))
  # def test_get_batch_restore_from_list
  #
  # end

  def test_get_medusa_id
    mock_dynamodb = Minitest::Mock.new
    expr_attr_vals = { ":file_type" => Settings.aws.dynamodb.current_id, }
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :file_type"
    args_verification = [Settings.aws.dynamodb.medusa_db_id_table_name, 1, expr_attr_vals, key_cond_expr]
    query_resp = Object.new
    def query_resp.items = [{Settings.aws.dynamodb.file_id => "12345689"}]
    mock_dynamodb.expect(:query, query_resp, args_verification)
    medusa_id = BatchRestoreFiles.get_medusa_id(mock_dynamodb)
    assert_mock(mock_dynamodb)
    assert_equal(12345689, medusa_id)
  end

  def test_get_max_id
    mock_medusa_db = Minitest::Mock.new
    sql = "SELECT MAX(id) FROM cfs_files"
    mock_medusa_db.expect(:exec, [{"max" => "123456789123"}], [sql])
    max_id = BatchRestoreFiles.get_max_id(mock_medusa_db)
    assert_mock(mock_medusa_db)
    assert_equal(123456789123, max_id)
  end

  def test_evaluate_done
    assert_equal(true, BatchRestoreFiles.evaluate_done(1, 0))
    assert_equal(true, BatchRestoreFiles.evaluate_done(1, 1))
    assert_equal(false, BatchRestoreFiles.evaluate_done(0, 1))
  end

  def test_get_id_iterator
    #test when id + 1000 less than max id and less than remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(1, 5000, 0)
    assert_equal(true, continue)
    assert_equal(1001, id_itr)

    #test when id + 1000 equal to max id and less than remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(0, 1000, 0)
    assert_equal(false, continue)
    assert_equal(1000, id_itr)

    #test when id + 1000 greater than max id and less than remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(150, 1000, 0)
    assert_equal(false, continue)
    assert_equal(1000, id_itr)

    #test when id + 1000 less than max id and greater than remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(150, 400000, 19000)
    assert_equal(false, continue)
    assert_equal(1150, id_itr)

    #test when id + 1000 less than max id and equal to remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(0, 400000, 19000)
    assert_equal(false, continue)
    assert_equal(1000, id_itr)

    #test when id + 1000 less than max id and greater than remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(2150, 400000, 19000)
    assert_equal(false, continue)
    assert_equal(3150, id_itr)

    #test when id + 1000 equal to max id and greater than remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(2000, 3000, 19500)
    assert_equal(false, continue)
    assert_equal(3000, id_itr)

    #test when id + 1000 greater than max id and greater than remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(1500, 2000, 19000)
    assert_equal(false, continue)
    assert_equal(2000, id_itr)

    #test when id + 1000 greater than max id and equal to remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(1000, 1000, 18000)
    assert_equal(false, continue)
    assert_equal(1000, id_itr)

    #test when id + 1000 equal to max id and equal to remaining batch count
    id_itr, continue = BatchRestoreFiles.get_id_iterator(0, 1000, 19000)
    assert_equal(false, continue)
    assert_equal(1000, id_itr)
  end

  def test_get_file
    mock_medusa_db = Minitest::Mock.new
    id = 123
    sql = "SELECT * FROM cfs_files WHERE id=#{id.to_s}"
    file_exp = {"id" => "123", "name" => "test", "size" => 123456, "md5_sum" => "12345678901234567890123456789012", "cfs_directory_id" => 1}
    mock_medusa_db.expect(:exec, [file_exp], [sql])
    file_act = BatchRestoreFiles.get_file(mock_medusa_db, id)
    assert_mock(mock_medusa_db)
    assert_equal(file_exp, file_act)
  end

  def test_get_files_in_batches
    mock_medusa_db = Minitest::Mock.new
    id = 0
    id_iterator = 3
    sql = "SELECT * FROM cfs_files WHERE id>#{id.to_s} AND  id<=#{id_iterator}"
    files_ret = [{"id" => "1", "name" => "test", "size" => 123456, "md5_sum" => "12345678901234567890123456789012", "cfs_directory_id" => 1},
                 {"id" => "2", "name" => "test1", "size" => 234567, "md5_sum" => "23456789012345678901234567890123", "cfs_directory_id" => 1},
                 {"id" => "3", "name" => "test2", "size" => 345678, "md5_sum" => "34567890123456789012345678901234", "cfs_directory_id" => 2}
    ]
    mock_medusa_db.expect(:exec, files_ret, [sql])
    file_dirs_act, medusa_files_act = BatchRestoreFiles.get_files_in_batches(mock_medusa_db, id, id_iterator)

    m_file_1 = MedusaFile.new("test", "1", 1, "12345678901234567890123456789012")
    m_file_2 = MedusaFile.new("test1", "2", 1, "23456789012345678901234567890123")
    m_file_3 = MedusaFile.new("test2", "3", 2, "34567890123456789012345678901234")
    medusa_files_exp = [m_file_1, m_file_2, m_file_3]
    medusa_files_act = medusa_files_act
    file_dirs_exp = [1,2]

    assert_mock(mock_medusa_db)
    assert_equal(file_dirs_exp, file_dirs_act)
    assert_equal(medusa_files_exp, medusa_files_act)
  end

  def test_get_path
    skip
  end

  def test_get_path_hash
    skip
  end

  def test_generate_manifest
    skip
  end

  def test_put_medusa_id
    mock_dynamodb = Minitest::Mock.new
    id = "123"
    test_item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_id,
      Settings.aws.dynamodb.file_id => id.to_s,
    }
    args_verification = [Settings.aws.dynamodb.medusa_db_id_table_name, test_item]
    mock_dynamodb.expect(:put_item, [], args_verification)
    BatchRestoreFiles.put_medusa_id(mock_dynamodb, id)
    assert_mock(mock_dynamodb)
  end

  def test_put_batch_item
    mock_dynamodb = Minitest::Mock.new
    id = "123"
    key = "123/test.tst"
    checksum = "12345678901234567890123456789012"
    batch_item = BatchItem.new(key, id, checksum)
    puts batch_item.s3_key
    test_item = {
      Settings.aws.dynamodb.s3_key => batch_item.s3_key,
      Settings.aws.dynamodb.file_id => batch_item.file_id,
      Settings.aws.dynamodb.initial_checksum => batch_item.initial_checksum,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    args_verification = [Settings.aws.dynamodb.fixity_table_name, test_item]
    mock_dynamodb.expect(:put_item, [], args_verification)
    Time.stub(:now, Time.new(1)) do
      BatchRestoreFiles.put_batch_item(mock_dynamodb, batch_item)
      assert_mock(mock_dynamodb)
    end
  end

  def test_put_manifest
    mock_s3 = Minitest::Mock.new
    mock_file = Minitest::Mock.new
    manifest = "test-manifest.csv"
    key = "fixity/#{manifest}"
    s3_resp = Object.new
    def s3_resp.etag = "98765432109876543210987654321021"
    args_verification = []
    mock_s3.expect(:put_object, s3_resp, [Minitest::Mock, Settings.aws.s3.backup_bucket, key])
    File.stub(:new, mock_file) do
      etag_act = BatchRestoreFiles.put_manifest(mock_s3, manifest)
      assert_mock(mock_s3)
      assert_equal("98765432109876543210987654321021", etag_act)
    end
  end

  def test_get_batch_from_list
    skip
  end

  def test_restore_item
    id = "123"
    key = "123/test.tst"
    checksum = "12345678901234567890123456789012"
    batch_item = BatchItem.new(key, id, checksum)

    mock_s3 = Minitest::Mock.new
    mock_s3.expect(:restore_object, [], [Settings.aws.s3.backup_bucket, key] )
    test_item = {
      Settings.aws.dynamodb.s3_key => batch_item.s3_key,
      Settings.aws.dynamodb.file_id => batch_item.file_id,
      Settings.aws.dynamodb.initial_checksum => batch_item.initial_checksum,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.new(1).getutc.iso8601(3)
    }
    mock_dynamodb = Minitest::Mock.new
    mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.fixity_table_name, test_item])
    Time.stub(:now, Time.new(1)) do
      BatchRestoreFiles.restore_item(mock_dynamodb, mock_s3, batch_item)
      assert_mock(mock_s3)
      assert_mock(mock_dynamodb)
    end

  end

  def test_send_batch_job
    manifest = "test-manifest.csv"
    etag = "98765432109876543210987654321021"
    job_id = "job-123456789"
    token = 2
    BatchRestoreFiles.stub(:get_request_token, token) do
      mock_s3_control = Minitest::Mock.new
      mock_resp = Minitest::Mock.new
      mock_dynamodb = Minitest::Mock.new
      mock_resp.expect(:job_id, job_id)
      mock_s3_control.expect(:create_job,  mock_resp, [manifest, token+1, etag])
      mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.batch_job_ids_table_name,
                                                            {Settings.aws.dynamodb.job_id => job_id, }])
      mock_dynamodb.expect(:put_item, [], [Settings.aws.dynamodb.medusa_db_id_table_name,
                                                          {Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_request_token,
                                                          Settings.aws.dynamodb.file_id => (token+1).to_s,}])
      BatchRestoreFiles.send_batch_job(mock_dynamodb, mock_s3_control, manifest, etag)
      assert_mock(mock_s3_control)
      assert_mock(mock_dynamodb)
    end
  end

  def test_get_request_token
    mock_dynamodb = Minitest::Mock.new
    table_name = Settings.aws.dynamodb.medusa_db_id_table_name
    limit = 1
    expr_attr_values = { ":request_token" => Settings.aws.dynamodb.current_request_token,}
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :request_token"
    request_toke_exp = 123
    query_resp = Minitest::Mock.new
    query_resp.expect(:items, [{Settings.aws.dynamodb.file_id => "#{request_toke_exp}"}])
    query_resp.expect(:nil?, false)
    args_validation = [table_name, limit, expr_attr_values, key_cond_expr]
    mock_dynamodb.expect(:query, query_resp, args_validation)
    request_token_act = BatchRestoreFiles.get_request_token(mock_dynamodb)
    assert_mock(mock_dynamodb)
    assert_equal(request_toke_exp, request_token_act)
  end

  def test_put_request_token
    mock_dynamodb = Minitest::Mock.new
    token = "1"
    item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_request_token,
      Settings.aws.dynamodb.file_id => token.to_s,
    }
    args_verification = [Settings.aws.dynamodb.medusa_db_id_table_name, item]
    mock_dynamodb.expect(:put_item, [], args_verification)
    BatchRestoreFiles.put_request_token(mock_dynamodb, token)
    assert_mock(mock_dynamodb)
  end

  def test_put_job_id
    mock_dynamodb = Minitest::Mock.new
    job_id = "job-123456789"
    item = { Settings.aws.dynamodb.job_id => job_id, }
    args_verification = [Settings.aws.dynamodb.batch_job_ids_table_name, item]
    mock_dynamodb.expect(:put_item, [], args_verification)
    BatchRestoreFiles.put_job_id(mock_dynamodb, job_id)
    assert_mock(mock_dynamodb)
  end
end
