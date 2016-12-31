require 'thread/pool'
require 'open3'
require 'set'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_DEFAULT_SIZE = 10

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
    @thread_pool =
      options[:thread_pool] ||
      Thread.pool(options[:thread_pool_size] || THREAD_POOL_DEFAULT_SIZE)
  end

  def files
    command = [ TARSNAP, '-tvf', @archive, *@options[:tarsnap_options] ]
    command.push(@options[:directory]) if @options[:directory]

    files = {}
    dirs = Set.new

    Open3.popen3(*command) do |_, out, _|
      out.gets(nil).split("\n").each do |entry|
        (_, _, _, _, size, _, _, _, name) = entry.split(/\s+/, 9)
        if name.end_with?('/')
          dirs.add(name)
        else
          files[name] = size.to_i
        end
      end
    end

    empty_dirs = dirs.clone
    files.each { |f, _| empty_dirs.delete(File.dirname(f) + '/') }

    dirs.each do |dir|
      components = dir.split('/')[0..-2]
      components.each_with_index do |_, i|
        empty_dirs.delete(components[0, i + 1].join('/') + '/')
      end
    end

    empty_dirs.each { |dir| files[dir] = 0 }

    files
  end

  def file_groups
    groups = (1..@thread_pool.max).map { Group.new }
    files.sort { |a, b| b.last <=> a.last }.each do |file|
      groups.sort.last.add(*file)
    end
    groups.map(&:files)
  end

  def run
    file_groups.each do |chunk|
      @thread_pool.process do
        command = [ TARSNAP, '-xvf', @archive, *(@options[:tarsnap_options] + chunk) ]
        Open3.popen3(*command) do |_, _, err|
          while line = err.gets
            @@output_mutex.synchronize { warn line.chomp }
          end
        end
      end
    end

    @thread_pool.shutdown unless @options[:thread_pool]
  end
end
