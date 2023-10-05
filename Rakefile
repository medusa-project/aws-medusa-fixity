# frozen_string_literal: true

require 'rake/testtask'
require 'simplecov'
require_relative 'bin/set-test-vars'

Rake::TestTask.new(:test) do |t|
  t.libs << 'lib' << 'test'
  # t.libs << 'lib'
  t.test_files = FileList['test/*_test.rb']
end
