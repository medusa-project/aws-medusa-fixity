# frozen_string_literal: true

require 'pg'
require 'config'

require_relative 'fixity/batch_item'
require_relative 'fixity/dynamodb'
require_relative 'fixity/fixity_constants'
require_relative 'fixity/fixity_secrets'
require_relative 'fixity/fixity_utils'
require_relative 'fixity/medusa_file'
require_relative 'fixity/s3'
require_relative 'fixity/s3_control'

class BatchRestoreFiles
  MAX_BATCH_COUNT = 175000
  MAX_BATCH_SIZE = 16 * 1024**2 * MAX_BATCH_COUNT
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :s3, :s3_control, :dynamodb, :medusa_db

  def initialize(s3 = S3.new, dynamodb = Dynamodb.new, s3_control = S3Control.new, medusa_db = FixitySecrets::MEDUSA_DB)
    @s3 = s3
    @dynamodb = dynamodb
    @s3_control = s3_control
    @medusa_db = medusa_db
  end

  def batch_restore
    time_start = Time.now
    id = get_medusa_id
    return nil if id.nil?

    max_id = get_max_id
    return nil if max_id.nil?

    return if evaluate_done(id, max_id)

    batch_size = 0
    batch_count = 0
    manifest = "manifest-#{Time.now.strftime('%F-%H:%M')}.csv"
    batch_continue = true
    while batch_continue
      id_iterator, batch_continue = get_id_iterator(id, max_id, batch_count)
      file_directories, medusa_files, size = get_files_in_batches(id, id_iterator)
      batch_count += medusa_files.size
      batch_size += size
      id = id_iterator
      # next if (file_directories.nil? || file_directories.empty?) || (medusa_files.nil? || medusa_files.empty?)
      directories = get_path_hash(file_directories)
      batch = generate_manifest(manifest, medusa_files, directories)
      put_requests = @dynamodb.get_put_requests(batch)
      @dynamodb.batch_write_items(Settings.aws.dynamodb.fixity_table_name, put_requests)
    end

    put_medusa_id(id)

    time_end = Time.now
    duration = time_end - time_start

    log_message = "Get batch duration to process #{batch_count} files of size: #{batch_size} :#{duration}"
    FixityConstants::LOGGER.info(log_message)

    return if batch_count.zero?

    etag = put_manifest(manifest)
    send_batch_job(manifest, etag)
  end

  def batch_restore_from_list(list)
    time_start = Time.now
    batch_size = 0
    manifest = "manifest-#{Time.now.strftime('%F-%H:%M')}.csv"
    file_directories, medusa_files, size = get_batch_from_list(list)
    batch_size += size
    directories = get_path_hash(file_directories)
    batch = generate_manifest(manifest, medusa_files, directories)

    put_requests = @dynamodb.get_put_requests(batch)
    @dynamodb.batch_write_items(Settings.aws.dynamodb.fixity_table_name, put_requests)

    time_end = Time.now
    duration = time_end - time_start

    log_message = "Get batch duration to process #{list.size} of size: #{batch_size} files: #{duration}"
    FixityConstants::LOGGER.info(log_message)

    etag = put_manifest(manifest)
    send_batch_job(manifest, etag)
  end

  def batch_restore_expired_items
    manifest = 'manifest-expired-files.csv'
    etag = put_manifest(manifest)
    send_batch_job(manifest, etag)
  end

  def get_medusa_id
    table_name = Settings.aws.dynamodb.medusa_db_id_table_name
    limit = 1
    expr_attr_vals = { ':file_type' => Settings.aws.dynamodb.current_id }
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :file_type"
    query_resp = @dynamodb.query(table_name, limit, expr_attr_vals, key_cond_expr)
    return nil if query_resp.nil? || query_resp.items.empty?

    query_resp.items[0][Settings.aws.dynamodb.file_id].to_i
  end

  def get_max_id
    max_resp = @medusa_db.exec('SELECT MAX(id) FROM cfs_files')
    max_resp.first['max'].to_i
  end

  def evaluate_done(id, max_id)
    done = id >= max_id
    done_message = 'FIXITYDONE: fixity id matches maximum file id in medusa'
    FixityConstants::LOGGER.error(done_message) if done
    done
  end

  def get_id_iterator(id, max_id, batch_count)
    temp_itr = id + 1000
    count_left = id + (MAX_BATCH_COUNT - batch_count)
    if (temp_itr < max_id) && (temp_itr < count_left)
      [temp_itr, true]
    elsif temp_itr >= max_id
      [max_id, false]
    else
      [count_left, false]
    end
  end

  def get_file(id)
    file_result = @medusa_db.exec_params('SELECT * FROM cfs_files WHERE id=$1', [{ value: id.to_s }])
    file_result.first
  end

  def get_files_in_batches(id, id_iterator)
    # TODO: add batch size as class variable to keep track of file sizes
    batch_size = 0
    medusa_files = []
    file_directories = []
    file_result = @medusa_db.exec_params('SELECT * FROM cfs_files WHERE id>$1 AND  id<=$2', [{ value: id.to_s },
                                                                                             { value: id_iterator.to_s }])
    file_result.each do |file_row|
      next if file_row.nil?

      file_id = file_row['id']
      directory_id = file_row['cfs_directory_id']
      name = file_row['name']
      batch_size += file_row['size'].to_i
      initial_checksum = file_row['md5_sum']
      file_directories.push(directory_id)
      medusa_files.push(MedusaFile.new(name, file_id, directory_id, initial_checksum))
    end
    [file_directories.uniq, medusa_files, batch_size]
  end

  def get_path(directory_id, path)
    while directory_id
      dir_result = @medusa_db.exec_params('SELECT * FROM cfs_directories WHERE id=$1', [{ value: directory_id }])
      dir_row = dir_result.first
      dir_path = dir_row['path']
      path.prepend(dir_path, '/')
      directory_id = dir_row['parent_id']
      parent_type = dir_row['parent_type']
      break if parent_type != 'CfsDirectory'
    end
    FixityUtils.escape(path)
  end

  def get_path_hash(file_directories)
    directories = {}
    file_directories.each do |directory_id|
      path = String.new
      file_directory = directory_id
      while directory_id
        dir_result = @medusa_db.exec_params('SELECT * FROM cfs_directories WHERE id=$1', [{ value: directory_id }])
        dir_row = dir_result.first
        dir_path = dir_row['path']
        path.prepend(dir_path, '/')
        directory_id = dir_row['parent_id']
        parent_type = dir_row['parent_type']
        break if parent_type != 'CfsDirectory'
      end
      directories[file_directory] = path
    end
    directories
  end

  def generate_manifest(manifest, medusa_files, directories)
    batch = []
    medusa_files.each do |medusa_file|
      directory_path = directories[medusa_file.directory_id.to_s]
      path = directory_path + medusa_file.name
      s3_key = FixityUtils.escape(path)
      open(manifest, 'a') { |f|
        f.puts "#{Settings.aws.s3.backup_bucket},#{s3_key}"
      }
      batch_hash = {
        Settings.aws.dynamodb.s3_key => s3_key,
        Settings.aws.dynamodb.file_id => medusa_file.file_id,
        Settings.aws.dynamodb.initial_checksum => medusa_file.initial_checksum,
        Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
        Settings.aws.dynamodb.last_updated => Time.now.getutc.iso8601(3)
      }
      batch.push(batch_hash)
    end
    batch
  end

  def put_medusa_id(id)
    table_name = Settings.aws.dynamodb.medusa_db_id_table_name
    item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_id,
      Settings.aws.dynamodb.file_id => id.to_s
    }
    @dynamodb.put_item(table_name, item)
  end

  def put_batch_item(batch_item)
    table_name = Settings.aws.dynamodb.fixity_table_name
    item = {
      Settings.aws.dynamodb.s3_key => batch_item.s3_key,
      Settings.aws.dynamodb.file_id => batch_item.file_id,
      Settings.aws.dynamodb.initial_checksum => batch_item.initial_checksum,
      Settings.aws.dynamodb.restoration_status => Settings.aws.dynamodb.requested,
      Settings.aws.dynamodb.last_updated => Time.now.getutc.iso8601(3)
    }
    @dynamodb.put_item(table_name, item)
  end

  def put_manifest(manifest)
    body = File.new(manifest)
    key = "fixity/#{manifest}"
    s3_resp = @s3.put_object(body, Settings.aws.s3.fixity_bucket, key)
    s3_resp.etag
  end

  def get_batch_from_list(list)
    batch_size = 0
    medusa_files = []
    file_directories = []
    list.each do |id|
      file_row = get_file(id)
      if file_row.nil?
        FixityConstants::LOGGER.error("File with id #{id} not found in medusa DB")
        next
      end
      file_id = file_row['id']
      directory_id = file_row['cfs_directory_id']
      name = file_row['name']
      batch_size += file_row['size'].to_i
      initial_checksum = file_row['md5_sum']
      file_directories.push(directory_id)
      medusa_files.push(MedusaFile.new(name, file_id, directory_id, initial_checksum))
    end
    [file_directories.uniq, medusa_files, batch_size]
  end

  def restore_expired_item(batch_item)
    open('manifest-expired-files.csv', 'a') { |f|
      f.puts "#{Settings.aws.s3.backup_bucket},#{batch_item.s3_key}"
    }
    put_batch_item(batch_item)
  end

  def restore_item(batch_item)
    key = FixityUtils.unescape(batch_item.s3_key)
    @s3.restore_object(@dynamodb, Settings.aws.s3.backup_bucket, key, batch_item.file_id)
    put_batch_item(batch_item)
  end

  def send_batch_job(manifest, etag)
    token = get_request_token + 1
    resp = @s3_control.create_job(manifest, token, etag)
    job_id = resp.job_id
    put_job_id(job_id)
    batch_job_message = "Batch restore job sent with id #{job_id}"
    FixityConstants::LOGGER.info(batch_job_message)
    put_request_token(token)
  end

  def get_request_token
    table_name = Settings.aws.dynamodb.medusa_db_id_table_name
    limit = 1
    expr_attr_values = { ':request_token' => Settings.aws.dynamodb.current_request_token }
    key_cond_expr = "#{Settings.aws.dynamodb.id_type} = :request_token"
    query_resp = @dynamodb.query(table_name, limit, expr_attr_values, key_cond_expr)
    return nil if query_resp.nil? || query_resp.items.empty?

    query_resp.items[0][Settings.aws.dynamodb.file_id].to_i
  end

  def put_request_token(token)
    table_name = Settings.aws.dynamodb.medusa_db_id_table_name
    item = {
      Settings.aws.dynamodb.id_type => Settings.aws.dynamodb.current_request_token,
      Settings.aws.dynamodb.file_id => token.to_s
    }
    @dynamodb.put_item(table_name, item)
  end

  def put_job_id(job_id)
    table_name = Settings.aws.dynamodb.batch_job_ids_table_name
    item = { Settings.aws.dynamodb.job_id => job_id }
    @dynamodb.put_item(table_name, item)
  end

  def max_batch_count
    MAX_BATCH_COUNT
  end
end
