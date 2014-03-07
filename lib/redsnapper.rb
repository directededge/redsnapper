require 'thread/pool'
require 'open3'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_SIZE = 25
  MAX_FILES_PER_JOB = 50

  def initialize(archive, options = {})
    @archive = archive
    @options = options
  end

  def file_groups
    command = [ TARSNAP, '-tf', @archive, *@options[:tarsnap_options] ]
    command.push(@options[:directory]) if @options[:directory]

    files = IO.popen(command) do |io|
      io.gets(nil).split.reject { |f| f.end_with?('/') }
    end

    files.each_slice([ (files.size.to_f / THREAD_POOL_SIZE).ceil, MAX_FILES_PER_JOB].min).to_a
  end

  def run
    pool = Thread.pool(THREAD_POOL_SIZE)
    mutex = Mutex.new

    file_groups.each do |chunk|
      pool.process do
        command = [ TARSNAP, '-xvf', @archive, *@options[:tarsnap_options], *chunk ]
        Open3.popen3(*command) do |_, _, err|
          while line = err.gets
            mutex.synchronize { warn line }
          end
        end
      end
    end

    pool.shutdown
  end
end
