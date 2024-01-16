# frozen_string_literal: true
require_relative 'fixity/dynamodb'

class ProcessResults
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", ENV['RUBY_ENV']))
  attr_accessor :dynamodb

  S3_KEY = Settings.aws.dynamodb.s3_key
  FILE_ID = Settings.aws.dynamodb.file_id
  INIT_CHECKSUM = Settings.aws.dynamodb.initial_checksum
  CALC_CHECKSUM = Settings.aws.dynamodb.calculated_checksum
  LAST_UPDATED = Settings.aws.dynamodb.last_updated

  def initialize(dynamodb = Dynamodb.new)
    @dynamodb = dynamodb
  end

  def create_fixity_mismatch_csv
    csv_name = "fixity-mismatch-#{Time.now.strftime('%F')}.csv"
    headers = [S3_KEY, FILE_ID, INIT_CHECKSUM, CALC_CHECKSUM, LAST_UPDATED]
    CSV.open(csv_name, 'w') do |csv|
      csv << headers
    end
    csv_name
  end

  def mismatch_resp
    table_name = Settings.aws.dynamodb.fixity_table_name
    index_name = Settings.aws.dynamodb.mismatch_index_name
    expr_names = {
      '#S3' => S3_KEY,
      '#FI' => FILE_ID,
      '#IC' => INIT_CHECKSUM,
      '#CC' => CALC_CHECKSUM,
      '#LU' => LAST_UPDATED,
    }
    proj_expr = '#S3, #FI, #IC, #CC, #LU'
    @dynamodb.scan_index(table_name, index_name, expr_names, proj_expr)
  end

  def populate_fixity_mismatch_csv(csv_name, mismatch_resp)
    CSV.open(csv_name, 'a') do |csv|
      mismatch_resp.items.each do |item|
        csv << [item[S3_KEY], item[FILE_ID].to_i, item[INIT_CHECKSUM], item[CALC_CHECKSUM], item[LAST_UPDATED]]
      end
    end
  end

  def generate_fixity_mismatch_csv
    csv_name = create_fixity_mismatch_csv
    mismatch_response = mismatch_resp
    populate_fixity_mismatch_csv(csv_name, mismatch_response)
  end

end
