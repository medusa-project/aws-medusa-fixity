# frozen_string_literal: true

class BatchItem
  attr_accessor :s3_key, :file_id, :initial_checksum

  def initialize(s3_key, file_id, initial_checksum)
    @s3_key = s3_key
    @file_id = file_id
    @initial_checksum = initial_checksum
  end
end
