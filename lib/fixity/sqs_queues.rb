require 'aws-sdk-sqs'

require_relative 'fixity_constants.rb'

class SqsQueues

  def self.send_messages
    queue_url = FixityConstants::SQS_CLIENT_WEST.get_queue_url(queue_name: "aws-to-fixity-local").queue_url
    message = {FixityConstants::FILE_ID => 1,
               FixityConstants::S3_KEY => "123/456/789/text.txt",
               FixityConstants::RESTORATION_STATUS => FixityConstants::RESTORE_COMPLETED,
              }
    FixityConstants::SQS_CLIENT_WEST.send_message({
      queue_url: FixityConstants::S3_QUEUE_URL,
      message_body: message.to_json,
      message_attributes: {}
    })

  end

  def self.get_messages
    response = FixityConstants::SQS_CLIENT_WEST.receive_message(queue_url: FixityConstants::S3_QUEUE_URL, max_number_of_messages: 10)
    response.messages.each do |message|
      puts JSON.parse(message.body)
      puts message.receipt_handle
    end
    message = JSON.parse(response.data.messages[0].body)
    FixityConstants::SQS_CLIENT_WEST.delete_message({queue_url: FixityConstants::S3_QUEUE_URL, receipt_handle: response.data.messages[0].receipt_handle})
    file_id = message[FixityConstants::FILE_ID]
    s3_key = message[FixityConstants::S3_KEY]
    restoration_status = message[FixityConstants::RESTORATION_STATUS]
    puts file_id
    puts s3_key
    puts restoration_status
  end

end
