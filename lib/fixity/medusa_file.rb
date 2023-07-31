# frozen_string_literal: true

class MedusaFile
  attr_accessor :name, :file_id, :directory_id, :initial_checksum

  def initialize(name, file_id, directory_id, initial_checksum)
    @name = name
    @file_id = file_id
    @directory_id = directory_id
    @initial_checksum = initial_checksum
  end

  def ==(other)
    # return true if self is equal to other_object, false otherwise
    name == other.name && file_id == other.file_id && directory_id == other.directory_id && initial_checksum == other.initial_checksum
  end
end
