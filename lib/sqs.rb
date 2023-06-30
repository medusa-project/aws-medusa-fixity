# frozen_string_literal: true
require 'aws-sdk-sqs'
require 'config'
require 'json'
require_relative 'fixity/fixity_constants.rb'
class Sqs
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :sqs_client
  def initialize(region, sqs_client = Aws::SQS::Client.new(region: Settings.aws.region_west))
    @sqs_client = (region == 'test') ? sqs_client : Aws::SQS::Client.new(region: region)
  end

  def send_message(file_id, checksum, found, status, error_message)
    checksums = {Settings.aws.sqs.md5 => checksum}
    parameters = {Settings.aws.sqs.checksums => checksums, Settings.aws.sqs.found => found}
    passthrough = {Settings.aws.sqs.cfs_file_id => file_id, Settings.aws.sqs.cfs_file_class => Settings.aws.sqs.cfs_file}
    if error_message.nil?
      message = {Settings.aws.sqs.action => Settings.aws.sqs.file_fixity,
                 Settings.aws.sqs.status => status,
                 Settings.aws.sqs.parameters => parameters,
                 Settings.aws.sqs.passthrough => passthrough}
    else
      message = {Settings.aws.sqs.action => FixityConstants::FILE_FIXITY,
                 Settings.aws.sqs.status => status,
                 Settings.aws.sqs.error_message => error_message,
                 Settings.aws.sqs.parameters => parameters,
                 Settings.aws.sqs.passthrough => passthrough}
    end
    puts message.to_json

    @sqs_client.send_message({
      queue_url: Settings.aws.sqs.medusa_queue_url,
      message_body: message.to_json,
      message_attributes: {}
    })
  end

  def

end
