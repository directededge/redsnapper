require 'thread/pool'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_SIZE = 20
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

    file_groups.each do |chunk|
      pool.process do
        unless system(TARSNAP, '-xvf', @archive, *@options[:tarsnap_options], *chunk)
          # mutex.syncronize { warn "Error extracting #{file}" }
        end
      end
    end

    pool.shutdown
  end
end
