# frozen_string_literal: true

class MedusaFile
  def initialize(name, file_id, directory_id, initial_checksum)
    @name = name
    @file_id = file_id
    @directory_id = directory_id
    @initial_checksum = initial_checksum
  end

  def name
    @name
  end

  def file_id
    @file_id
  end

  def directory_id
    @directory_id
  end

  def initial_checksum
    @initial_checksum
  end
end
