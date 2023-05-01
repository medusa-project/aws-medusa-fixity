# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'

require_relative 'fixity/fixity_constants.rb'
require_relative 'fixity/medusa_item'

class RestoreFiles
  #MAX_BATCH_SIZE = max bytes to process in 24 hours
  def self.get_batch
    #get information from medusa DB for a batch of files to be restored(File ID, S3 key, initial checksum)
    # medusa_item => make object with file_id, s3_key, initial_checksum
    batch = []
    #query medusa and add files to batch
    # Implement medusa querying
    s3_key = "test-key"
    file_id = "test-id"
    initial_checksum = "test-initial-checksum"
    medusa_item = MedusaItem.new(s3_key, file_id, initial_checksum)
    batch.push(medusa_item)
    restore_batch(batch)
  end

  def self.restore_batch(batch)
    # make restore requests to S3 glacier for next batch of files to be processed
    # files may take up to 48 hours to restore and are only available for 24 to save costs
    batch.each do |fixity_item|
      #skip s3 restoration locally, minio doesn't support restoration
      begin
        FixityConstants::DYNAMODB_CLIENT.put_item({
          table_name: FixityConstants::TABLE_NAME,
          item: {
            FixityConstants::S3_KEY => fixity_item.s3_key,
            FixityConstants::FILE_ID => fixity_item.file_id,
            FixityConstants::INITIAL_CHECKSUM => fixity_item.initial_checksum,
            FixityConstants::RESTORATION_STATUS => FixityConstants::REQUESTED,
            FixityConstants::LAST_UPDATED => Time.now.getutc.iso8601(3)
          }
        })
      rescue StandardError => e
        error_message = "Error putting item in dynamodb table for #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.message}"
        FixityConstants::LOGGER.info(error_message)
      end
    end
  end
end
