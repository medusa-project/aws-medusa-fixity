require 'aws-sdk-s3'
require 'digest'
require 'aws-sdk-dynamodb'
require 'cgi'
require 'config'

require_relative 'fixity/dynamodb'
require_relative 'fixity/fixity_constants.rb'
require_relative 'send_message.rb'

class Fixity
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  MEGABYTE = 1024 * 1024

  def self.run_fixity
    #get object info from dynamodb
    fixity_item = get_fixity_item

    s3_key = fixity_item[Settings.aws.dynamo_db.s3_key]
    file_id = fixity_item[Settings.aws.dynamo_db.file_id].to_i
    initial_checksum = fixity_item[Settings.aws.dynamo_db.initial_checksum]
    file_size = fixity_item[Settings.aws.dynamo_db.file_size]

    message = "FIXITY: File id #{file_id}, S3 key #{s3_key}"
    FixityConstants::LOGGER.info(message)

    #update dynamodb table to remove fixity ready and set fixity status
    update_fixity_ready(s3_key)

    #compare calculated checksum with initial checksum
    calculated_checksum = calculate_checksum(s3_key, file_id, file_size)

    fixity_outcome = (calculated_checksum == initial_checksum) ? Settings.aws.dynamo_db.match : Settings.aws.dynamo_db.mismatch

    case fixity_outcome
    when Settings.aws.dynamo_db.match
      #update dynamodb calculated checksum, fixity status, fixity verification
      update_fixity_match(s3_key, calculated_checksum)
    when Settings.aws.dynamo_db.mismatch
      #update dynamodb mismatch, calculated checksum, fixity status, fixity verification
      update_fixity_mismatch(s3_key, calculated_checksum)
    else
      outcome_message = "Fixity outcome not recognized"
      FixityConstants::LOGGER.error(outcome_message)
    end

    # send sqs to medusa with result
    #SendMessage.send_message(file_id, calculated_checksum, FixityConstants::TRUE, FixityConstants::SUCCESS, nil )
  end

  def self.run_fixity_batch
    #get fixity ready batch info from dynamodb
    fixity_batch = get_fixity_batch
    return nil if fixity_batch.nil? || fixity_batch.empty?

    #update dynamodb table to remove fixity ready and set fixity status
    update_fixity_ready_batch = get_update_fixity_ready_batch(fixity_batch)
    return nil if update_fixity_ready_batch.empty?

    Dynamodb.batch_write_items(FixityConstants::FIXITY_TABLE_NAME, update_fixity_ready_batch)

    fixity_batch.each do |fixity_item|

      s3_key = fixity_item[Settings.aws.dynamo_db.s3_key]
      file_id = fixity_item[Settings.aws.dynamo_db.file_id].to_i
      initial_checksum = fixity_item[Settings.aws.dynamo_db.initial_checksum]
      file_size = fixity_item[Settings.aws.dynamo_db.file_size]

      message = "FIXITY: File id #{file_id}, S3 key #{s3_key}"
      FixityConstants::LOGGER.info(message)

      #compare calculated checksum with initial checksum
      calculated_checksum = calculate_checksum(s3_key, file_id, file_size)

      fixity_outcome = (calculated_checksum == initial_checksum) ? Settings.aws.dynamo_db.match : Settings.aws.dynamo_db.mismatch

      case fixity_outcome
      when Settings.aws.dynamo_db.match
        #update dynamodb calculated checksum, fixity status, fixity verification
        update_fixity_match(s3_key, calculated_checksum)
      when Settings.aws.dynamo_db.mismatch
        #update dynamodb mismatch, calculated checksum, fixity status, fixity verification
        update_fixity_mismatch(s3_key, calculated_checksum)
      else
        outcome_message = "Fixity outcome not recognized"
        FixityConstants::LOGGER.error(outcome_message)
      end

      # send sqs to medusa with result
      #SendMessage.send_message(file_id, calculated_checksum, FixityConstants::TRUE, FixityConstants::SUCCESS, nil )
    end
  end

  def self.get_fixity_item
    #TODO expand to query multiple fixity items at a time
    begin
      query_resp = FixityConstants::DYNAMODB_CLIENT.query({
        table_name: Settings.aws.dynamo_db.fixity_table_name,
        index_name: Settings.aws.dynamo_db.index_name,
        limit: 1,
        scan_index_forward: true,
        expression_attribute_values: {
          ":ready" => Settings.aws.dynamo_db.true,
        },
        key_condition_expression: "#{Settings.aws.dynamo_db.fixity_ready} = :ready",
      })
      return nil if query_resp.items[0].nil?
    rescue StandardError => e
      error_message = "Error querying dynamodb table: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    query_resp.items[0]
  end

  def self.get_fixity_batch
    #TODO expand to query multiple fixity items at a time
    begin
      query_resp = FixityConstants::DYNAMODB_CLIENT.query({
        table_name: Settings.aws.dynamo_db.fixity_table_name,
        index_name: Settings.aws.dynamo_db.index_name,
        limit: 25,
        scan_index_forward: true,
        expression_attribute_values: {
          ":ready" => Settings.aws.dynamo_db.true,
        },
        key_condition_expression: "#{Settings.aws.dynamo_db.fixity_ready} = :ready",
      })
      return nil if query_resp.items.nil?
    rescue StandardError => e
      error_message = "Error querying dynamodb table: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    query_resp.items
  end

  def self.get_update_fixity_ready_batch(fixity_batch)
    put_requests = []
    fixity_batch.each do |fixity_item|
      fixity_item[Settings.aws.dynamo_db.last_updated] = Time.now.getutc.iso8601(10)
      fixity_item[Settings.aws.dynamo_db.fixity_status] = Settings.aws.dynamo_db.calculating
      fixity_item.delete(Settings.aws.dynamo_db.fixity_ready)
      put_requests << {
        put_request: {
          item: fixity_item
        }
      }
    end
    return [put_requests]
  end
  def self.update_fixity_ready(dynamodb, s3_key)
    key = { Settings.aws.dynamo_db.s3_key => s3_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamo_db.calculating,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp "\
                  "REMOVE #{Settings.aws.dynamo_db.fixity_ready}"
    dynamodb.update_item(Settings.aws.dynamo_db.fixity_table_name, key, {}, expr_attr_values, update_expr)
  end

  def self.calculate_checksum(s3_key, file_id, file_size)
    # stream s3 object through md5 calculation in 16 mb chunks
    # compare with initial md5 checksum and send medusa result via sqs
    md5 = Digest::MD5.new
    download_size_start = 0
    download_size_end = 16*MEGABYTE
    begin
      while download_size_start < file_size
        object_part = FixityConstants::S3_CLIENT.get_object({
          bucket: Settings.aws.s3.backup_bucket, # required
          key: CGI.unescape(s3_key), # required
          range: "bytes=#{download_size_start}-#{download_size_end}"
        })
        md5 << object_part.body.read
        download_size_end = download_size_end+1
        download_size_start = download_size_end
        download_size_end = download_size_end+16*MEGABYTE
      end
    rescue StandardError => e
      error_message = "Error calculating md5 for object #{s3_key} with ID #{file_id}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      update_fixity_error(s3_key)
      exit
    end
    md5.hexdigest
  end

  def self.update_fixity_match(dynamodb, s3_key, calculated_checksum)
    key = { Settings.aws.dynamo_db.s3_key => s3_key }
    expr_attr_values = {
      ":fixity_status" => Settings.aws.dynamo_db.done,
      ":fixity_outcome" => Settings.aws.dynamo_db.match,
      ":calculated_checksum" => calculated_checksum,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamo_db.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp"
    dynamodb.update_item(Settings.aws.dynamo_db.fixity_table_name, key, {}, expr_attr_values, update_expr)

  end

  def self.update_fixity_mismatch(dynamodb, s3_key, calculated_checksum)
    key = { Settings.aws.dynamo_db.s3_key => s3_key }
    expr_attr_values = {
      ":mismatch" => Settings.aws.dynamo_db.true,
      ":fixity_status" => Settings.aws.dynamo_db.done,
      ":fixity_outcome" => Settings.aws.dynamo_db.mismatch,
      ":calculated_checksum" => calculated_checksum,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.fixity_outcome} = :fixity_outcome, " \
                      "#{Settings.aws.dynamo_db.calculated_checksum} = :calculated_checksum, " \
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp, " \
                      "#{Settings.aws.dynamo_db.mismatch} = :mismatch"
    dynamodb.update_item(Settings.aws.dynamo_db.fixity_table_name, key, {}, expr_attr_values, update_expr)
  end

  def self.update_fixity_error(dynamodb, s3_key)
    key = { Settings.aws.dynamo_db.s3_key => s3_key }
    expr_attr_names = {
      "#E" => Settings.aws.dynamo_db.error,
    }
    expr_attr_values = {
      ":error" => Settings.aws.dynamo_db.true,
      ":fixity_status" => Settings.aws.dynamo_db.error,
      ":timestamp" => Time.now.getutc.iso8601(3)
    }
    update_expr = "SET #{Settings.aws.dynamo_db.fixity_status} = :fixity_status, "\
                      "#{Settings.aws.dynamo_db.last_updated} = :timestamp, " \
                      "#E = :error"
    dynamodb.update_item(Settings.aws.dynamo_db.fixity_table_name, key, expr_attr_names, expr_attr_values, update_expr)
  end
end

