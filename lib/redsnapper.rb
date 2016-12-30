require 'thread/pool'
require 'open3'
require 'set'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_SIZE = 10

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
    @thread_pool_size = options[:thread_pool_size] || THREAD_POOL_SIZE
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

    files.each { |f, _| dirs.delete(File.dirname(f) + '/') }

    empty_dirs = dirs.clone

    dirs.each do |dir|
      components = dir.split('/')
      components.each_with_index do |component, i|
        empty_dirs.delete(components[0, i].join('/') + '/')
      end
    end

    empty_dirs.each { |dir| files[dir] = 0 }

    files
  end

  def file_groups
    groups = (1..@thread_pool_size).map { Group.new }
    files.sort { |a, b| b.last <=> a.last }.each do |file|
      groups.sort.last.add(*file)
    end
    groups.map(&:files)
  end

  def run
    pool = Thread.pool(@thread_pool_size)
    mutex = Mutex.new

    file_groups.each do |chunk|
      pool.process do
        command = [ TARSNAP, '-xvf', @archive, *(@options[:tarsnap_options] + chunk) ]
        Open3.popen3(*command) do |_, _, err|
          while line = err.gets
            mutex.synchronize { warn line.chomp }
          end
        end
      end
    end

    pool.shutdown
  end
end
