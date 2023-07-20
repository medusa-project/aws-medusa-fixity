# frozen_string_literal: true
require 'aws-sdk-sqs'
require 'config'
require 'json'
require_relative 'fixity/fixity_constants.rb'
class MedusaSqs
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :medusa_sqs_client

  def initialize(medusa_sqs_client = FixityConstants::SQS_CLIENT_EAST)
    @medusa_sqs_client = medusa_sqs_client
  end
  def send_medusa_message(file_id, checksum, found, status, error_message=nil)
    checksums = {Settings.aws.sqs.md5 => checksum}
    parameters = {Settings.aws.sqs.checksums => checksums, Settings.aws.sqs.found => found}
    passthrough = {Settings.aws.sqs.cfs_file_id => file_id, Settings.aws.sqs.cfs_file_class => Settings.aws.sqs.cfs_file}
    if error_message.nil?
      message = {Settings.aws.sqs.action => Settings.aws.sqs.file_fixity,
                 Settings.aws.sqs.status => status,
                 Settings.aws.sqs.parameters => parameters,
                 Settings.aws.sqs.passthrough => passthrough}
    else
      message = {Settings.aws.sqs.action => Settings.aws.sqs.file_fixity,
                 Settings.aws.sqs.status => status,
                 Settings.aws.sqs.error_message => error_message,
                 Settings.aws.sqs.parameters => parameters,
                 Settings.aws.sqs.passthrough => passthrough}
    end
    log_message = "SQS message: #{message.to_json}"
    FixityConstants::LOGGER.info(log_message)
    @medusa_sqs_client.send_message({
      queue_url: Settings.aws.sqs.medusa_queue_url,
      message_body: message.to_json,
      message_attributes: {}
    })
  end
end
