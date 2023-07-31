# frozen_string_literal: true

class Pid
  def self.running?(pid)
    Process.getpgid(pid)
    true
  rescue Errno::ESRCH
    false
  end
end
