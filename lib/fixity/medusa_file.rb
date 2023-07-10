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

  def ==(obj)
    # return true if self is equal to other_object, false otherwise
    self.name == obj.name && self.file_id == obj.file_id && self.directory_id == obj.directory_id && self.initial_checksum == obj.initial_checksum
  end
end
