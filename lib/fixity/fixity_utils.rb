# frozen_string_literal: true

require 'cgi'

class FixityUtils
  FIXITY_COUNT_FILE = "#{ENV['TMP_HOME']}/fixity_count.txt"
  def self.escape(s3_key)
    CGI.escape(s3_key).gsub(/~|(%2F)/, '%2F' => '/', '~' => '%7E')
  end

  def self.unescape(s3_key)
    CGI.unescape(s3_key)
  end

  def self.increment_fixity_count
    count = FixityUtils.get_fixity_count + 1
    File.write(FIXITY_COUNT_FILE, count)
  end

  def self.decrement_fixity_count
    current_count = FixityUtils.get_fixity_count
    count = current_count.positive? ? current_count - 1 : 0
    File.write(FIXITY_COUNT_FILE, count)
  end

  def self.get_fixity_count
    number = File.read(FIXITY_COUNT_FILE).to_i
  end
end
