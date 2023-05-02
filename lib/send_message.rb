# frozen_string_literal: true
require 'aws-sdk-sqs'
require 'json'
require_relative 'fixity/fixity_constants.rb'
class SendMessage
  def self.send_message(file_id, checksum, found, status, error_message)
    checksums = {FixityConstants::MD5 => checksum}
    parameters = {FixityConstants::CHECKSUMS => checksums, FixityConstants::FOUND => found}
    passthrough = {FixityConstants::CFS_FILE_ID => file_id, FixityConstants::CFS_FILE_CLASS => FixityConstants::CFS_FILE}
    if error_message.nil?
      message = {FixityConstants::ACTION => FixityConstants::FILE_FIXITY,
                 FixityConstants::STATUS => status,
                 FixityConstants::PARAMETERS => parameters,
                 FixityConstants::PASSTHROUGH => passthrough}
    else
      message = {FixityConstants::ACTION => FixityConstants::FILE_FIXITY,
                 FixityConstants::STATUS => status,
                 FixityConstants::ERROR_MESSAGE => error_message,
                 FixityConstants::PARAMETERS => parameters,
                 FixityConstants::PASSTHROUGH => passthrough}
    end
    puts message.to_json

    FixityConstants::SQS_CLIENT_EAST.send_message({
      queue_url: FixityConstants::MEDUSA_QUEUE_URL,
      message_body: message.to_json,
      message_attributes: {}
    })
  end
end
