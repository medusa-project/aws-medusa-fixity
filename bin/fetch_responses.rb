#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative '../lib/restoration_event'

restoration_event = RestorationEvent.new
restoration_event.handle_message
