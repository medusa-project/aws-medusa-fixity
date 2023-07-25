require 'digest'
require 'config'
require 'csv'

require_relative 'fixity/dynamodb'
require_relative 'fixity/fixity_constants'
require_relative 'fixity/fixity_utils'
require_relative 'fixity/s3'
require_relative 'medusa_sqs'

class Fixity
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  MEGABYTE = 1024 * 1024
  attr_accessor :s3, :dynamodb, :medusa_sqs

  def initialize(s3 = S3.new, dynamodb = Dynamodb.new, medusa_sqs = MedusaSqs.new)
    @s3 = s3
    @dynamodb = dynamodb
    @medusa_sqs = medusa_sqs
  end

  def run_fixity
    #TODO add test
    #get object info from dynamodb
    fixity_item = get_fixity_item

    s3_key = fixity_item[Settings.aws.dynamodb.s3_key]
    file_id = fixity_item[Settings.aws.dynamodb.file_id].to_i
    initial_checksum = fixity_item[Settings.aws.dynamodb.initial_checksum]
    file_size = fixity_item[Settings.aws.dynamodb.file_size]

    message = "FIXITY: File id #{file_id}, S3 key #{s3_key}"
    FixityConstants::LOGGER.info(message)

    #update dynamodb table to remove fixity ready and set fixity status
    update_fixity_ready(s3_key)

    #compare calculated checksum with initial checksum
    calculated_checksum, error_message = calculate_checksum(s3_key, file_id, file_size)

    handle_outcome(initial_checksum, calculated_checksum, s3_key)

    # send sqs to medusa with result
    create_medusa_message(file_id, calculated_checksum, error_message)
  end

  def run_fixity_batch
    #TODO add test
    #get fixity ready batch info from dynamodb
    fixity_batch = get_fixity_batch
    return nil if fixity_batch.nil? || fixity_batch.empty?

    #update dynamodb table to remove fixity ready and set fixity status
    update_fixity_ready_batch = get_update_fixity_ready_batch(fixity_batch)
    return nil if update_fixity_ready_batch.empty?

    @dynamodb.batch_write_items(Settings.aws.dynamodb.fixity_table_name, update_fixity_ready_batch)

    fixity_batch.each do |fixity_item|

      s3_key = fixity_item[Settings.aws.dynamodb.s3_key]
      file_id = fixity_item[Settings.aws.dynamodb.file_id].to_i
      initial_checksum = fixity_item[Settings.aws.dynamodb.initial_checksum]
      file_size = fixity_item[Settings.aws.dynamodb.file_size]

      message = "FIXITY: File id #{file_id}, S3 key #{s3_key}"
      FixityConstants::LOGGER.info(message)

      #compare calculated checksum with initial checksum
      calculated_checksum, error_message = calculate_checksum(s3_key, file_id, file_size)

      handle_outcome(initial_checksum, calculated_checksum, s3_key)

      # send sqs to medusa with result
      create_medusa_message(file_id, calculated_checksum, error_message)
    end
  end

  def run_fixity_from_csv(csv_file)
    #TODO add test
    time_start = Time.now

    fixity_items = CSV.new(File.read(csv_file))
    row_num = 0
    fixity_items.each do |row|
      row_num+=1
      bucket, key = row
      update_fixity_ready(key)
      expr_attr_vals = {":key" => key,}
      key_cond_expr = "#{Settings.aws.dynamodb.s3_key} = :key"
      fixity_item = @dynamodb.query(Settings.aws.dynamodb.fixity_table_name, 1, expr_attr_vals, key_cond_expr)
      file_id = fixity_item.items[0][Settings.aws.dynamodb.file_id].to_i
      file_size = fixity_item.items[0][Settings.aws.dynamodb.file_size]
      initial_checksum = fixity_item.items[0][Settings.aws.dynamodb.initial_checksum]
      calculated_checksum, error_message = calculate_checksum(key, file_id, file_size)

      handle_outcome(initial_checksum, calculated_checksum, key)

      # send sqs to medusa with result
      create_medusa_message(file_id, calculated_checksum, error_message)
    end
    time_end = Time.now
    duration = time_end - time_start
    FixityConstants::LOGGER.info("Fixity from CSV duration to process #{row_num} files: #{duration}")
  end

  def get_fixity_item
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.index_name
    limit = 1
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    query_resp = @dynamodb.query_with_index(table_name, index_name, limit, expr_attr_vals, key_cond_expr)
    return nil if query_resp.nil? || query_resp.items.empty?
    return query_resp.items[0]
  end

  def get_fixity_batch
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.index_name
    limit = 25
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    query_resp = @dynamodb.query_with_index(table_name, index_name, limit, expr_attr_vals, key_cond_expr)
    return nil if query_resp.nil? ||  query_resp.empty? || query_resp.items.empty?
    return query_resp.items
  end

  def get_update_fixity_ready_batch(fixity_batch)
    put_requests = []
    fixity_batch.each do |fixity_item|
      fixity_item[Settings.aws.dynamodb.last_updated] = Time.now.getutc.iso8601(3)
      fixity_item[Settings.aws.dynamodb.fixity_status] = Settings.aws.dynamodb.calculating
      fixity_item.delete(Settings.aws.dynamodb.fixity_ready)
      put_requests << {
        put_request: {
          item: fixity_item
        }
      }
    end
    return [put_requests]
  end
  def update_fixity_ready(s3_key)
    key = { Settings.aws.dynamodb.s3_key => s3_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamodb.calculating,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp "\
                  "REMOVE #{Settings.aws.dynamodb.fixity_ready}"
    @dynamodb.update_item(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr)
  end

  def calculate_checksum(s3_key, file_id, file_size)
    # stream s3 object through md5 calculation in 16 mb chunks
    # compare with initial md5 checksum and send medusa result via sqs
    if file_size.nil?
      error_message = "Error calculating md5 for object #{s3_key} with ID #{file_id}: file size nil"
      FixityConstants::LOGGER.error(error_message)
      return nil, error_message
    end
    md5 = Digest::MD5.new
    download_size_start = 0
    download_size_end = 16*MEGABYTE
    key = FixityUtils.unescape(s3_key)
    begin
      while download_size_start < file_size
        range = "bytes=#{download_size_start}-#{download_size_end}"
        object_part = @s3.get_object_with_byte_range(Settings.aws.s3.backup_bucket, key, range)
        md5 << object_part.body.read
        download_size_end = download_size_end+1
        download_size_start = download_size_end
        download_size_end = download_size_end+16*MEGABYTE
      end
    rescue StandardError => e
      error_message = "Error calculating md5 for object #{s3_key} with ID #{file_id}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      update_fixity_error(s3_key)
      return nil, error_message
    end
    return md5.hexdigest, nil
  end

  def handle_outcome(initial_checksum, calculated_checksum, key)
    fixity_outcome = (calculated_checksum == initial_checksum) ? Settings.aws.dynamodb.match : Settings.aws.dynamodb.mismatch
    case fixity_outcome
    when Settings.aws.dynamodb.match
      #update dynamodb calculated checksum, fixity status, fixity verification
      update_fixity_match(key, calculated_checksum)
    when Settings.aws.dynamodb.mismatch
      #update dynamodb mismatch, calculated checksum, fixity status, fixity verification
      update_fixity_mismatch(key, calculated_checksum)
    else
      outcome_message = "Fixity outcome not recognized"
      FixityConstants::LOGGER.error(outcome_message)
    end
  end

  def update_fixity_match(s3_key, calculated_checksum)
    key = { Settings.aws.dynamodb.s3_key => s3_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamodb.done,
      ":fixity_outcome" => Settings.aws.dynamodb.match,
      ":calculated_checksum" => calculated_checksum,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamodb.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp"
    @dynamodb.update_item(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr)
  end

  def update_fixity_mismatch(s3_key, calculated_checksum)
    key = { Settings.aws.dynamodb.s3_key => s3_key }
    expr_attr_values = {
      ":mismatch" => Settings.aws.dynamodb.true,
      ":fixity_status" => Settings.aws.dynamodb.done,
      ":fixity_outcome" => Settings.aws.dynamodb.mismatch,
      ":calculated_checksum" => calculated_checksum,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamodb.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, " \
                      "#{Settings.aws.dynamodb.mismatch} = :mismatch"
    @dynamodb.update_item(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr)
  end

  def update_fixity_error(s3_key)
    key = { Settings.aws.dynamodb.s3_key => s3_key }
    expr_attr_names = {
      "#E" => Settings.aws.dynamodb.error,
    }
    expr_attr_values = {
      ":error" => Settings.aws.dynamodb.true,
      ":fixity_status" => Settings.aws.dynamodb.error,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp, " \
                      "#E = :error"
    @dynamodb.update_item_with_names(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_names, expr_attr_values, update_expr)
  end

  def create_medusa_message(file_id, calculated_checksum, error_message)
    if calculated_checksum.nil?
      @medusa_sqs.send_medusa_message(file_id, calculated_checksum, true, Settings.aws.sqs.failure, error_message)
    else
      @medusa_sqs.send_medusa_message(file_id, calculated_checksum, true, Settings.aws.sqs.success)
    end
  end
end

