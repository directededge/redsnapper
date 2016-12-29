require 'thread/pool'
require 'open3'
require 'set'

class Array
  def interleaved_slices(max_per_slice)
    raise ArgumentError unless max_per_slice >= 1

    count = (size.to_f / max_per_slice).ceil

    slices = (1..count).map { [] }
    each_with_index do |v, i|
      slices[i % count].push(v)
    end

    slices
  end
end

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_SIZE = 10
  MAX_FILES_PER_JOB = 1000

  def initialize(archive, options = {})
    @archive = archive
    @options = options
    @max_files_per_job = options[:max_files_per_job] || MAX_FILES_PER_JOB
    @thread_pool_size = options[:thread_pool_size] || THREAD_POOL_SIZE
  end

  def file_groups
    command = [ TARSNAP, '-tf', @archive, *@options[:tarsnap_options] ]
    command.push(@options[:directory]) if @options[:directory]

    files = []
    dirs = Set.new

    Open3.popen3(*command) do |_, out, _|
      out.gets(nil).split("\n").each do |entry|
        if entry.end_with?('/')
          dirs.add(entry)
        else
          files.push(entry)
        end
      end
    end

    files.each { |f| dirs.delete(File.dirname(f) + '/') }
    empty_dirs = dirs.clone

    dirs.each do |dir|
      components = dir.split('/')
      components.each_with_index do |component, i|
        empty_dirs.delete(components[0, i].join('/') + '/')
      end
    end

    files.push(*empty_dirs)
    files_per_slice = [ (files.size.to_f / @thread_pool_size).ceil, @max_files_per_job ].min
    files.interleaved_slices(files_per_slice)
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
