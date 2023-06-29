# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'config'
require_relative 'fixity_constants'
class Dynamodb
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  @dynamodb_client = Aws::DynamoDB::Client.new(region: Settings.aws.region_west)
  attr_accessor :dynamodb_client
  def initialize(dynamodb_client)
    @dynamodb_client = dynamodb_client
  end
  def self.get_put_requests(batch)
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
        array_itr = array_itr + 1
        put_requests[array_itr] = []
      end
    end
    return put_requests
  end

  #TODO handle returned unprocessed_items
  def batch_write_items(table_name, write_requests)
    return nil if write_requests.nil? || write_requests.empty?
    write_requests.each do |write_request|
      begin
        resp = @dynamodb_client.batch_write_item({
          request_items: { # required
            table_name => write_request
          }
        })
      rescue StandardError => e
        error_message = "Error putting batch items in dynamodb table: #{e.message}"
        FixityConstants::LOGGER.error(error_message)
        error_info_message = "Write requests not put in dynamodb table: #{write_requests}"
        FixityConstants::LOGGER.info(error_info_message)
      end
    end
  end

  def update_item(table_name, key, expr_attr_names, expr_attr_values, update_expr)
    begin
      FixityConstants::DYNAMODB_CLIENT.update_item({
        table_name: table_name,
        key: key,
        expression_attribute_names: expr_attr_names,
        expression_attribute_values: expr_attr_values,
        update_expression: update_expr
      })
    rescue StandardError => e
      error_message = "Error updating #{table_name} key: #{key} with update expression: #{update_expr}. #{e.message} "
      FixityConstants::LOGGER.error(error_message)
    end
  end
end
