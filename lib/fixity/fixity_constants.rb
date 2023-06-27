# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'aws-sdk-s3'
require 'aws-sdk-s3control'
require 'aws-sdk-sqs'

class FixityConstants
  #AWS
  REGION_WEST = "us-west-2"
  REGION_EAST = "us-east-2"
  BULK = "Bulk"
  BACKUP_BUCKET = "medusa-demo-main-backup"
  BACKUP_BUCKET_ARN = "arn:aws:s3:::medusa-demo-main-backup"
  S3_CLIENT = Aws::S3::Client.new(region: REGION_WEST)
  SQS_CLIENT_WEST = Aws::SQS::Client.new(region: REGION_WEST)
  SQS_CLIENT_EAST = Aws::SQS::Client.new(region: REGION_EAST)
  DYNAMODB_CLIENT = Aws::DynamoDB::Client.new(region: REGION_WEST)
  MEDUSA_QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/721945215539/fixity-to-medusa-demo"
  S3_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/721945215539/s3-to-fixity-demo"
  LOGGER = Logger.new('/home/ec2-user/logs/fixity.log', 'daily')
  S3_CONTROL_CLIENT = Aws::S3Control::Client.new(region: REGION_WEST)
  ACCOUNT_ID = "721945215539"
  BATCH_ROLE_ARN = "arn:aws:iam::721945215539:role/fixity-demo-batch-restore-role"
  BATCH_PREFIX = "fixity/BatchRestoreReports"


  #DYNAMODB
  FIXITY_TABLE_NAME = "FixityDemoTable"
  RESTORATION_ERRORS_TABLE_NAME = "FixityDemoRestorationErrorsTable"
  MEDUSA_DB_ID_TABLE_NAME = "FixityDemoMedusaDBIdTable"
  INDEX_NAME = "FixityProcessingQueue"
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
  ID_TYPE = "IdType"

  #MEDUSA DB IDS
  CURRENT_ID = "CurrentId"
  MAX_ID = "MaxId"
  CURRENT_REQUEST_TOKEN = "CurrentRequestToken"
  JOB_ID = "JobId"
  PROCESSED = "Processed"

  #ERROR DYNAMODB
  ERR_CODE = "ErrorCode"
  HTTPS_STATUS_CODE = "HTTPSStatusCode"


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
