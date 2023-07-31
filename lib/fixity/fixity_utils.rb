# frozen_string_literal: true

require 'cgi'

class FixityUtils
  def self.escape(s3_key)
    CGI.escape(s3_key).gsub(/~|(%2F)/, '%2F' => '/', '~' => '%7E')
  end

  def self.unescape(s3_key)
    CGI.unescape(s3_key)
  end
end
