require 'digest'
require 'config'

require_relative 'fixity/dynamodb'
require_relative 'fixity/fixity_constants'
require_relative 'fixity/fixity_utils'
require_relative 'fixity/s3'
require_relative 'medusa_sqs'

class Fixity
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  MEGABYTE = 1024 * 1024

  def self.run_fixity
    #get object info from dynamodb
    dynamodb = Dynamodb.new
    s3 = S3.new
    medusa_sqs = MedusaSqs.new

    fixity_item = get_fixity_item(dynamodb)

    s3_key = fixity_item[Settings.aws.dynamodb.s3_key]
    file_id = fixity_item[Settings.aws.dynamodb.file_id].to_i
    initial_checksum = fixity_item[Settings.aws.dynamodb.initial_checksum]
    file_size = fixity_item[Settings.aws.dynamodb.file_size]

    message = "FIXITY: File id #{file_id}, S3 key #{s3_key}"
    FixityConstants::LOGGER.info(message)

    #update dynamodb table to remove fixity ready and set fixity status
    update_fixity_ready(dynamodb_client, s3_key)

    #compare calculated checksum with initial checksum
    calculated_checksum = calculate_checksum(s3, s3_key, file_id, file_size, dynamodb)

    fixity_outcome = (calculated_checksum == initial_checksum) ? Settings.aws.dynamodb.match : Settings.aws.dynamodb.mismatch

    case fixity_outcome
    when Settings.aws.dynamodb.match
      #update dynamodb calculated checksum, fixity status, fixity verification
      update_fixity_match(dynamodb, s3_key, calculated_checksum)
    when Settings.aws.dynamodb.mismatch
      #update dynamodb mismatch, calculated checksum, fixity status, fixity verification
      update_fixity_mismatch(dynamodb, s3_key, calculated_checksum)
    else
      outcome_message = "Fixity outcome not recognized"
      FixityConstants::LOGGER.error(outcome_message)
    end

    # send sqs to medusa with result
    medusa_sqs.send_medusa_message(file_id, calculated_checksum, Settings.aws.dynamodb.true, Settings.aws.sqs.success)
  end

  def self.run_fixity_batch
    dynamodb = Dynamodb.new
    s3 = S3.new
    medusa_sqs = MedusaSqs.new

    #get fixity ready batch info from dynamodb
    fixity_batch = get_fixity_batch(dynamodb)
    return nil if fixity_batch.nil? || fixity_batch.empty?

    #update dynamodb table to remove fixity ready and set fixity status
    update_fixity_ready_batch = get_update_fixity_ready_batch(fixity_batch)
    return nil if update_fixity_ready_batch.empty?

    dynamodb.batch_write_items(Settings.aws.dynamodb.fixity_table_name, update_fixity_ready_batch)

    fixity_batch.each do |fixity_item|

      s3_key = fixity_item[Settings.aws.dynamodb.s3_key]
      file_id = fixity_item[Settings.aws.dynamodb.file_id].to_i
      initial_checksum = fixity_item[Settings.aws.dynamodb.initial_checksum]
      file_size = fixity_item[Settings.aws.dynamodb.file_size]

      message = "FIXITY: File id #{file_id}, S3 key #{s3_key}"
      FixityConstants::LOGGER.info(message)

      #compare calculated checksum with initial checksum
      calculated_checksum = calculate_checksum(s3, s3_key, file_id, file_size, dynamodb)

      fixity_outcome = (calculated_checksum == initial_checksum) ? Settings.aws.dynamodb.match : Settings.aws.dynamodb.mismatch

      case fixity_outcome
      when Settings.aws.dynamodb.match
        #update dynamodb calculated checksum, fixity status, fixity verification
        update_fixity_match(dynamodb, s3_key, calculated_checksum)
      when Settings.aws.dynamodb.mismatch
        #update dynamodb mismatch, calculated checksum, fixity status, fixity verification
        update_fixity_mismatch(dynamodb, s3_key, calculated_checksum)
      else
        outcome_message = "Fixity outcome not recognized"
        FixityConstants::LOGGER.error(outcome_message)
      end

      # send sqs to medusa with result
      medusa_sqs.send_medusa_message(file_id, calculated_checksum, Settings.aws.dynamodb.true, Settings.aws.sqs.success)
    end
  end

  def self.get_fixity_item(dynamodb)
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.index_name
    limit = 1
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    query_resp = dynamodb.query_with_index(table_name, index_name, limit, expr_attr_vals, key_cond_expr)
    return nil if query_resp.nil? || query_resp.items.empty?
    return query_resp.items[0]
  end

  def self.get_fixity_batch(dynamodb)
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.index_name
    limit = 25
    expr_attr_vals = {":ready" => Settings.aws.dynamodb.true,}
    key_cond_expr = "#{Settings.aws.dynamodb.fixity_ready} = :ready"
    query_resp = dynamodb.query_with_index(table_name, index_name, limit, expr_attr_vals, key_cond_expr)
    return nil if query_resp.nil? ||  query_resp.empty? || query_resp.items.empty?
    return query_resp.items
  end

  def self.get_update_fixity_ready_batch(fixity_batch)
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
  def self.update_fixity_ready(dynamodb, s3_key)
    key = { Settings.aws.dynamodb.s3_key => s3_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamodb.calculating,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamodb.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamodb.last_updated} = :timestamp "\
                  "REMOVE #{Settings.aws.dynamodb.fixity_ready}"
    dynamodb.update_item(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr)
  end

  def self.calculate_checksum(s3, s3_key, file_id, file_size, dynamodb)
    # stream s3 object through md5 calculation in 16 mb chunks
    # compare with initial md5 checksum and send medusa result via sqs
    md5 = Digest::MD5.new
    download_size_start = 0
    download_size_end = 16*MEGABYTE
    key = FixityUtils.unescape(s3_key)
    begin
      while download_size_start < file_size
        range = "bytes=#{download_size_start}-#{download_size_end}"
        object_part = s3.get_object_with_byte_range(Settings.aws.s3.backup_bucket, key, range)
        md5 << object_part.body.read
        download_size_end = download_size_end+1
        download_size_start = download_size_end
        download_size_end = download_size_end+16*MEGABYTE
      end
    rescue StandardError => e
      error_message = "Error calculating md5 for object #{s3_key} with ID #{file_id}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      update_fixity_error(dynamodb, s3_key)
      exit
    end
    md5.hexdigest
  end

  def self.update_fixity_match(dynamodb, s3_key, calculated_checksum)
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
    dynamodb.update_item(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr)

  end

  def self.update_fixity_mismatch(dynamodb, s3_key, calculated_checksum)
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
    dynamodb.update_item(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_values, update_expr)
  end

  def self.update_fixity_error(dynamodb, s3_key)
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
    dynamodb.update_item_with_names(Settings.aws.dynamodb.fixity_table_name, key, expr_attr_names, expr_attr_values, update_expr)
  end
end

