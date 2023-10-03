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
    manifest_table = CSV.new(File.read(csv_file))
    manifest_table.each do |row|
      _bucket, key = row
      key = escape(key)
      open(manifest, 'a') { |f|
        f.puts "#{Settings.aws.s3.backup_bucket},#{key}"
      }
    end
  end
end
