#!/usr/bin/env ruby

ENV['RUBY_ENV'] = 'test'
ENV['RUBY_HOME'] = ENV['IS_DOCKER'] == 'true' ? '/fixity' : '/Users/gschmitt/workspace/aws-medusa-fixity'
ENV['TEST_HOME'] = "#{ENV['RUBY_HOME']}/test"
ENV['LOGGER_HOME'] = "#{ENV['RUBY_HOME']}/logs"
