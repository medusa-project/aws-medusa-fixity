# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-sqs'

class FixityConstants
  #AWS
  REGION_WEST = "us-west-2"
  REGION_EAST = "us-east-2"
  BULK = "Bulk"
  BACKUP_BUCKET = "medusa-demo-main-backup"
  S3_CLIENT = Aws::S3::Client.new(region: REGION_WEST)
  SQS_CLIENT_WEST = Aws::SQS::Client.new(region: REGION_WEST)
  SQS_CLIENT_EAST = Aws::SQS::Client.new(region: REGION_EAST)
  DYNAMODB_CLIENT = Aws::DynamoDB::Client.new(region: REGION_WEST)
  MEDUSA_QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/721945215539/fixity-to-medusa-demo"
  S3_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/721945215539/s3-to-fixity-demo"
  LOGGER = Logger.new('/Users/gschmitt/workspace/aws-medusa-fixity/logs/fixity.log', 'daily')

  #DYNAMODB
  TABLE_NAME = "FixityVerifications"
  INDEX_NAME = "FixityCalculation"
  S3_KEY = "S3Key"
  FILE_ID = "FileId"
  INITIAL_CHECKSUM = "InitialChecksum"
  RESTORATION_STATUS = "RestorationStatus"
  FIXITY_STATUS = "FixityStatus"
  FIXITY_OUTCOME = "FixityOutcome"
  CALCULATED_CHECKSUM = "CalculatedChecksum"
  LAST_UPDATED = "LastUpdated"
  FIXITY_READY = "FixityReady"
  FILE_SIZE = "FileSize"

  #FIXITY STATUS
  CALCULATING = "CALCULATING"
  DONE = "DONE"
  ERROR = "ERROR"

  #RESTORATION STATUS
  REQUESTED = "REQUESTED"
  RESTORE_POSTED = "ObjectRestore:Post"
  INITIATED = "INITIATED"
  RESTORE_COMPLETED = "ObjectRestore:Completed"
  COMPLETED = "COMPLETED"
  RESTORE_DELETED = "ObjectRestore:Delete"
  EXPIRED = "EXPIRED"

  #FIXITY OUTCOMES
  MATCH = "MATCH"
  MISMATCH = "MISMATCH"

  #FIXITY READY
  TRUE = "true"
  FALSE = "false"

  #MEDUSA SQS VALUES
  ACTION = "action"
  FILE_FIXITY = "file_fixity"
  STATUS = "status"
  SUCCESS = "success"
  FAILURE = "failure"
  ERROR_MESSAGE = "error_message"
  PARAMETERS = "parameters"
  CHECKSUMS = "checksums"
  MD5 = "md5"
  FOUND = "found"
  PASSTHROUGH = "pass_through"
  CFS_FILE_ID = "cfs_file_id"
  CFS_FILE_CLASS = "cfs_file_class"
  CFS_FILE = "CfsFile"
end
