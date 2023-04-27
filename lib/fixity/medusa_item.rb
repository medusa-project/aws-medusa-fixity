# frozen_string_literal: true

class MedusaItem
  def initialize(s3_key, file_id, initial_checksum)
    @s3_key = s3_key
    @file_id = file_id
    @initial_checksum = initial_checksum
  end

  def s3_key
    @s3_key
  end

  def file_id
    @file_id
  end

  def initial_checksum
    @initial_checksum
  end
end
