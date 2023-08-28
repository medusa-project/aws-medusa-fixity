require 'minitest/autorun'
require 'config'

require_relative '../lib/fixity/fixity_utils'

class TestFixityUtils < Minitest::Test
  Config.load_and_set_settings(Config.setting_files("#{ENV['RUBY_HOME']}/config", 'test'))

  # Special characters defined https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
  def test_escape
    base_path = '1/2/3/4/5/'
    test_file = 'test.txt'
    special_chars = { '&' => '%26', '$' => '%24', '@' => '%40', '=' => '%3D', ';' => '%3B', ':' => '%3A', '+' => '%2B',
                      ' ' => '+', ',' => '%2C', '?' => '%3F', '{' => '%7B', '^' => '%5E', '}' => '%7D', '%' => '%25',
                      '`' => '%60', ']' => '%5D', '>' => '%3E', '[' => '%5B', '~' => '%7E', '#' => '%23', '|' => '%7C',
                      '<' => '%3C' }
    special_chars.each_key do |spec_char|
      unesc_path = base_path + spec_char + test_file
      esc_path = base_path + special_chars[spec_char] + test_file
      act_path = FixityUtils.escape(unesc_path)
      assert_equal(esc_path, act_path)
    end
  end

  def test_unescape
    base_path = '1/2/3/4/5/'
    test_file = 'test.txt'
    special_chars = { '%26' => '&', '%24' => '$', '%40' => '@', '%3D' => '=', '%3B' => ';', '%3A' => ':', '%2B' => '+',
                      '+' => ' ', '%2C' => ',', '%3F' => '?', '%7B' => '{', '%5E' => '^', '%7D' => '}', '%25' => '%',
                      '%60' => '`', '%5D' => ']', '%3E' => '>', '%5B' => '[', '%7E' => '~', '%23' => '#', '%7C' => '|',
                      '%3C' => '<' }
    special_chars.each_key do |spec_char|
      esc_path = base_path + spec_char + test_file
      unesc_path = base_path + special_chars[spec_char] + test_file
      act_path = FixityUtils.unescape(esc_path)
      assert_equal(unesc_path, act_path)
    end
  end
end
