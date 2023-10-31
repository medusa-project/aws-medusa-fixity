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

  def self.compareCSV(expired, manifest, new_manifest)
    manifest_csv = CSV.new(File.read(manifest))
    manifest_keys = []
    manifest_csv.each do |row|
      _manifest_bucket, manifest_key = row
      manifest_keys.push(manifest_key)
    end
    expired_csv = CSV.new(File.read(expired))
    expired_csv.each do |row|
      expired_bucket, expired_key = row
      next unless manifest_keys.include?(expired_key)

      open(new_manifest, 'a') { |f|
        f.puts "#{expired_bucket},#{expired_key}"
      }
    end
  end
end
