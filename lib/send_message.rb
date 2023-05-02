# frozen_string_literal: true
require 'aws-sdk-sqs'
require 'json'
require_relative 'fixity/fixity_constants.rb'
class SendMessage
  def self.send_message(file_id, checksum, found, status)
    checksums = {FixityConstants::MD5 => checksum}
    parameters = {FixityConstants::CHECKSUMS => checksums, FixityConstants::FOUND => found}
    passthrough = {FixityConstants::CFS_FILE_ID => file_id, FixityConstants::CFS_FILE_CLASS => FixityConstants::CFS_FILE}
    message = {FixityConstants::ACTION => FixityConstants::FILE_FIXITY,
               FixityConstants::STATUS => status,
               FixityConstants::PARAMETERS => parameters,
               FixityConstants::PASSTHROUGH => passthrough}
    puts message.to_json
    # FixityConstants::SQS_CLIENT_EAST.send_message({
    #   queue_url: FixityConstants::MEDUSA_QUEUE_URL,
    #   message_body: message.to_json,
    #   message_attributes: {}
    # })
  end
end
