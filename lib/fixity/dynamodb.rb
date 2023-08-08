# frozen_string_literal: true
#
require 'aws-sdk-dynamodb'
require 'config'
require_relative 'fixity_constants'
class Dynamodb
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :dynamodb_client

  def initialize(dynamodb_client = FixityConstants::DYNAMODB_CLIENT)
    @dynamodb_client = dynamodb_client
  end

  def put_item(table_name, item)
    @dynamodb_client.put_item({
                                table_name: table_name,
                                item: item
                              })
  rescue StandardError => e
    error_message = "Error putting items #{item} in dynamodb table #{table_name}: #{e.message}"
    FixityConstants::LOGGER.error(error_message)
  end

  def get_put_requests(batch)
    put_requests = []
    array_itr = 0
    put_requests[array_itr] = []
    batch.each do |batch_hash|
      put_requests[array_itr] << {
        put_request: {
          item: batch_hash
        }
      }
      if put_requests[array_itr].size == 25
        array_itr += 1
        put_requests[array_itr] = []
      end
    end
    put_requests.delete([])
    put_requests
  end

  # TODO: test handle returned unprocessed_items
  def batch_write_items(table_name, write_requests)
    return nil if write_requests.nil? || write_requests.empty?

    write_requests.each do |write_request|
      next if write_request.empty?

      resp = @dynamodb_client.batch_write_item({
                                                 request_items: { # required
                                                   table_name => write_request
                                                 }
                                               })
      retries = 0
      max_retries = 5
      until resp.unprocessed_items.empty?
        if retries <= max_retries
          retries += 1
          sleep 2**retries
          resp = dynamodb_client.batch_write_item(resp.unprocessed_items)
        else
          error_message = "Error handling unprocessed items: #{resp.unprocessed_items}"
          FixityConstants::LOGGER.error(error_message)
        end
      end
    rescue StandardError => e
      error_message = "Error putting batch items in dynamodb table: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
      error_info_message = "Write requests not put in dynamodb table: #{write_request}"
      FixityConstants::LOGGER.info(error_info_message)
    end
  end

  def update_item(table_name, key, expr_attr_values, update_expr, ret_val = 'NONE')
    begin
      update_resp = @dynamodb_client.update_item({
                                                   table_name: table_name,
                                                   key: key,
                                                   expression_attribute_values: expr_attr_values,
                                                   update_expression: update_expr,
                                                   return_values: ret_val
                                                 })
    rescue StandardError => e
      error_message = "Error updating #{table_name} key: #{key} with update expression: #{update_expr}. #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    update_resp
  end

  def update_item_with_names(table_name, key, expr_attr_names, expr_attr_values, update_expr, ret_val = 'NONE')
    begin
      update_resp = @dynamodb_client.update_item({
                                                   table_name: table_name,
                                                   key: key,
                                                   expression_attribute_names: expr_attr_names,
                                                   expression_attribute_values: expr_attr_values,
                                                   update_expression: update_expr,
                                                   return_values: ret_val
                                                 })
    rescue StandardError => e
      error_message = "Error updating #{table_name} key: #{key} with update expression: #{update_expr}. #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    update_resp
  end

  def query_with_index(table_name, index_name, limit, exp_attr_vals, key_cond_expr)
    begin
      query_resp = @dynamodb_client.query({
                                            table_name: table_name,
                                            index_name: index_name,
                                            limit: limit,
                                            scan_index_forward: true,
                                            expression_attribute_values: exp_attr_vals,
                                            key_condition_expression: key_cond_expr,
                                          })
    rescue StandardError => e
      error_message = "Error querying dynamodb table #{table_name} index #{index_name} with expression #{key_cond_expr}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    query_resp
  end

  def query(table_name, limit, expr_attr_vals, key_cond_expr)
    begin
      # Get medusa id to start next batch from dynamodb
      query_resp = @dynamodb_client.query({
                                            table_name: table_name,
                                            limit: limit,
                                            scan_index_forward: true,
                                            expression_attribute_values: expr_attr_vals,
                                            key_condition_expression: key_cond_expr
                                          })
    rescue StandardError => e
      # Error getting current request token
      error_message = "Error querying dynamodb table #{table_name} with expression #{key_cond_expr}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    query_resp
  end

  def scan(table_name, limit)
    begin
      scan_resp = @dynamodb_client.scan({
                                          table_name: table_name,
                                          limit: limit,
                                        })
    rescue StandardError => e
      error_message = "Error scanning dynamodb table #{table_name}: #{e.message}"
        FixityConstants::LOGGER.error(error_message)
    end
    scan_resp
  end

  def delete_item(key, table_name)
    begin
      resp = @dynamodb_client.delete_item({
                                            key: key,
                                            table_name: table_name,
                                          })
    rescue StandardError => e
      error_message = "Error deleting key #{key} from dynamodb table #{table_name}: #{e.message}"
      FixityConstants::LOGGER.error(error_message)
    end
    resp
  end
end
