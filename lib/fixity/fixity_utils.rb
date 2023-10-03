# frozen_string_literal: true

require 'cgi'
require 'csv'

class FixityUtils
  def self.escape(s3_key)
    CGI.escape(s3_key).gsub(/~|(%2F)/, '%2F' => '/', '~' => '%7E')
  end

  def self.unescape(s3_key)
    CGI.unescape(s3_key)
  end

  def self.escape_csv(csv_file, manifest)
    File.readlines(csv_file, chomp: true).each do |line|
      bucket, key = line.split(',', 2)
      key = escape(key)
      open(manifest, 'a') { |f|
        f.puts "#{bucket},#{key}"
      }
    end
  end
end
