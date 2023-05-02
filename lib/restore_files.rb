# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'

require_relative 'fixity/fixity_constants.rb'
require_relative 'fixity/medusa_item'
require_relative 'send_message.rb'

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
      begin
        FixityConstants::S3_CLIENT.restore_object({
          bucket: FixityConstants::BACKUP_BUCKET,
          key: fixity_item.s3_key,
          restore_request: {
            days: 1,
            glacier_job_parameters: {
              tier: FixityConstants::BULK
            },
          },
        })
      rescue Aws::S3::Errors::NoSuchKey => e
        #File not found in S3 bucket, don't add to dynamodb table (maybe add to separate table for investigation?)
        error_message = "Error getting object #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.message}"
        FixityConstants::LOGGER.error(error_message)
        SendMessage.send_message(file_id, nil, FixityConstants::FALSE, FixityConstants::FAILURE, error_message)

      rescue StandardError => e
        # Error requesting object restoration, add to dynamodb table for retry?
        # Send error message to medusa
        error_message = "Error getting object #{fixity_item.s3_key} with ID #{fixity_item.file_id}: #{e.message}"
        FixityConstants::LOGGER.error(error_message)

      end
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
