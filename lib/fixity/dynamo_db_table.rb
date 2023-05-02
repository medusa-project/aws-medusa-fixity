# frozen_string_literal: true
require 'aws-sdk-dynamodb'

class DynamoDbTable
  DYNAMO_DB_CLIENT = Aws::DynamoDB::Client.new(endpoint: "http://localhost:8000")
  def self.createTable
    dyanmodbCreateResp = DYNAMO_DB_CLIENT.create_table({
      attribute_definitions: [
        {
          attribute_name: "S3Key",
          attribute_type: "S",
        },
        {
          attribute_name: "FixityReady",
          attribute_type: "S",
        },
        {
          attribute_name: "LastUpdated",
          attribute_type: "S",
        },
        {
          attribute_name: "Error",
          attribute_type: "S",
        },
        {
          attribute_name: "MISMATCH",
          attribute_type: "S",
        },
      ],
      key_schema: [
        {
          attribute_name: "S3Key",
          key_type: "HASH",
        },
      ],
      table_name: "FixityVerifications",
      global_secondary_indexes: [
        {
          index_name: "FixityCalculation",
          key_schema: [
            {
              attribute_name: "FixityReady",
              key_type: "HASH",
            },
            {
              attribute_name: "LastUpdated",
              key_type: "RANGE"
            },
          ],
          projection: {
            projection_type: "ALL"
          },
        },
        {
          index_name: "FixityErrors",
          key_schema: [
            {
              attribute_name: "Error",
              key_type: "HASH",
            },
            {
              attribute_name: "LastUpdated",
              key_type: "RANGE"
            },
          ],
          projection: {
            projection_type: "ALL"
          },
        },
        {
          index_name: "FixityMismatch",
          key_schema: [
            {
              attribute_name: "MISMATCH",
              key_type: "HASH",
            },
            {
              attribute_name: "LastUpdated",
              key_type: "RANGE"
            },
          ],
          projection: {
            projection_type: "ALL"
          },
        },
      ],
      tags: [
        key: "Service",
        value: "Fixity"
      ],
      billing_mode: "PAY_PER_REQUEST"
    })
    puts dyanmodbCreateResp
  end

  def self.deleteTable
    resp = DYNAMO_DB_CLIENT.delete_table({
      table_name: "FixityVerifications", # required
    })
  end

  def self.scanTable
    scan_resp= DYNAMO_DB_CLIENT.scan({
      table_name: "FixityVerifications",
    })
    scan_resp.items.each do |resp|
      puts "S3Key: #{resp["S3Key"]}"
      puts "FileId: #{resp["FileId"]}"
      puts "InitialChecksum: #{resp["InitialChecksum"]}"
      puts "LastUpdated: #{resp["LastUpdated"]}"
      puts "RestorationStatus: #{resp["RestorationStatus"]}"
      puts "FixityStatus: #{resp["FixityStatus"]}"
      puts "FixityOutcome: #{resp["FixityOutcome"]}"
      puts "CalculatedChecksum: #{resp["CalculatedChecksum"]}"
      puts "FixityReady: #{resp["FixityReady"]}"
    end
  end

  def self.describeTable
    resp = DYNAMO_DB_CLIENT.describe_table({
      table_name: "FixityVerifications", # required
    })
    puts "Describe Table: #{resp.to_h}"
  end

  def self.getTime
    puts Time.now.getutc.iso8601(3)
  end

  # createTable
  # getTime
  scanTable
  # deleteTable
  # describeTable
end
