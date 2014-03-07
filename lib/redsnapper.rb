require 'thread/pool'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_SIZE = 20

  def initialize(archive, args = [])
    @archive = archive
    @args = args
  end

  def files
    @files ||= `#{TARSNAP} #{@args.join(' ')} -tf #{@archive}`.split.reject do |file|
      file.end_with?('/')
    end
  end

  def run
    pool = Thread.pool(THREAD_POOL_SIZE)
    mutex = Mutex.new

    files.each do |file|
      pool.process do
        unless system("#{TARSNAP} #{@args.join(' ')} -xf #{@archive} \"#{file}\"")
          mutex.syncronize { warn "Error extracting #{file}" }
        end
        mutex.synchronize { puts "-> #{file}" }
      end
    end

    pool.shutdown
  end
end
