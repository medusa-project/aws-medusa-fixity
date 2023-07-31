
class SetEnvVars
  def self.set_vars
    ENV['RUBY_ENV'] = 'demo'
    ENV['RUBY_HOME'] = '/home/ec2-user/aws-medusa-fixity'
    ENV['TEST_HOME'] = '/home/ec2-user/aws-medusa-fixity/test'
    ENV['BIN_HOME'] = '/home/ec2-user/aws-medusa-fixity/bin'
    ENV['TMP_HOME'] = '/home/ec2-user/aws-medusa-fixity/tmp'
    ENV['LOGGER_HOME'] = '/home/ec2-user/logs'
  end
end

