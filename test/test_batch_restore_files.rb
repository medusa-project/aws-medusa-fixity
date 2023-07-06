require 'minitest/autorun'
require 'config'
require 'json'

require_relative '../lib/batch_restore_files'

class TestBatchRestoreFiles < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", "test"))
end
