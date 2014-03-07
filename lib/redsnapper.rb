require 'thread/pool'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_SIZE = 20

  def initialize(archive, args = [])
    @archive = archive
    @args = args
  end

  def files
    files = `#{TARSNAP} -tf \"#{@archive}\" #{@args.join(' ')} opt`.split.reject do |file|
      file.end_with?('/')
    end
    files.each_slice([ (files.size.to_f / THREAD_POOL_SIZE).ceil, 20].min).to_a
  end

  def run
    pool = Thread.pool(THREAD_POOL_SIZE)
    mutex = Mutex.new

    files.each do |chunk|
      pool.process do
        block = chunk.map { |f| "\"#{f}\"" }.join(' ')
        unless system(TARSNAP, '-xvf', @archive, *@args, *chunk)
          # mutex.syncronize { warn "Error extracting #{file}" }
        end
      end
    end

    pool.shutdown
  end
end
