# frozen_string_literal: true
require 'aws-sdk-dynamodb'

require_relative 'fixity_constants'
class Dynamodb
  def self.put_batch_items_in_table(table_name, batch)
    put_requests = []
    batch.each do |batch_hash|
      put_requests << {
        put_request: {
          item: batch_hash
        }
      }
      if put_requests.size == 25
        batch_put_items(table_name, put_requests)
        put_requests.clear
      end
    end
    batch_put_items(table_name, put_requests)
  end

  #TODO handle returned unprocessed_items
  def self.batch_put_items(table_name, write_requests)
    return nil if write_requests.nil? || write_requests.empty?
    begin
      resp = FixityConstants::DYNAMODB_CLIENT.batch_write_item({
        request_items: { # required
          table_name => write_requests
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
