require 'thread/pool'
require 'open3'
require 'set'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_DEFAULT_SIZE = 10
  EXIT_ERROR = "tarsnap: Error exit delayed from previous errors.\n"

  @@output_mutex = Mutex.new

  class Group
    attr_reader :files, :size
    def initialize
      @files = []
      @size = 0
    end
    def add(name, size)
      @files << name
      @size += size
    end
    def <=>(other)
      other.size <=> size
    end
  end

  def initialize(archive, options = {})
    @archive = archive
    @options = options
    @thread_pool = Thread.pool(options[:thread_pool_size] || THREAD_POOL_DEFAULT_SIZE)
    @error = false
  end

  def files
    return @files if @files

    command = [ TARSNAP, '-tvf', @archive, *@options[:tarsnap_options] ]
    command.push(@options[:directory]) if @options[:directory]

    @files = {}

    Open3.popen3(*command) do |_, out, _|
      out.gets(nil).split("\n").each do |entry|
        (_, _, _, _, size, _, _, _, name) = entry.split(/\s+/, 9)
        @files[name] = {
          :size => size.to_i
        }
      end
    end

    @files
  end

  def empty_dirs(files, dirs)
    empty_dirs = dirs.clone
    files.each { |f| empty_dirs.delete(File.dirname(f) + '/') }
    dirs.each do |dir|
      components = dir.split('/')[0..-2]
      components.each_with_index do |_, i|
        empty_dirs.delete(components[0, i + 1].join('/') + '/')
      end
    end
    empty_dirs
  end

  def files_to_extract
    files_to_extract, dirs = files.partition { |f| !f.first.end_with?('/') }.map(&:to_h)
    empty_dirs(files_to_extract.keys, dirs.keys).each do |dir|
      files_to_extract[dir] = { :size => 0 }
    end
    files_to_extract
  end

  def file_groups
    groups = (1..@thread_pool.max).map { Group.new }
    files_to_extract.sort { |a, b| b.last[:size] <=> a.last[:size] }.each do |name, props|
      groups.sort.last.add(name, props[:size])
    end
    groups.map(&:files)
  end

  def run
    file_groups.each do |chunk|
      @thread_pool.process do
        command = [ TARSNAP, '-xvf', @archive, *(@options[:tarsnap_options] + chunk) ]
        Open3.popen3(*command) do |_, _, err|
          while line = err.gets
            if line == EXIT_ERROR
              @error = true
              next
            end
            @@output_mutex.synchronize { warn line.chomp }
          end
        end
      end
    end

    @thread_pool.shutdown
    @@output_mutex.synchronize { warn EXIT_ERROR } if @error
  end
end
