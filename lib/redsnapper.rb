require 'thread'
require 'open3'
require 'set'
require 'date'

class RedSnapper
  TARSNAP = 'tarsnap'
  THREAD_POOL_DEFAULT_SIZE = 10

  EXIT_ERROR = "tarsnap: Error exit delayed from previous errors.\n"
  NOT_OLDER_ERROR = "File on disk is not older; skipping.\n"

  GLOB_CHARS = '*?[]{}'

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
    @tpsize = options[:thread_pool_size] || THREAD_POOL_DEFAULT_SIZE
    @work_qs = (0...@tpsize).map do
      Queue.new
    end
    @thread_pool = (0...@tpsize).map do |i|
      Thread.new do
        chunk = @work_qs[i].pop()

        # The following gross hack works around this gross bug in BSD tar that tarsnap inherits:
        # https://github.com/Tarsnap/tarsnap/issues/329
        chunk.map! { |file| file.gsub(/([#{Regexp.escape(GLOB_CHARS)}])/) { |m| "\\#{m}" } }
        command = [ TARSNAP, '-xvf', @archive, *(@options[:tarsnap_options] + chunk) ]
        Open3.popen3(*command) do |_, _, err|
          while line = err.gets
            next if line.end_with?(NOT_OLDER_ERROR)
            if line == EXIT_ERROR
              @error = true
              next
            end
            @@output_mutex.synchronize { warn line.chomp }
          end
        end
      end
    end

    @error = false
  end

  def files
    return @files if @files

    command = [ TARSNAP, '-tvf', @archive, *@options[:tarsnap_options] ]
    command.push(@options[:directory]) if @options[:directory]

    @files = {}

    Open3.popen3(*command) do |_, out, _|
      out.gets(nil).split("\n").each do |entry|
        (_, _, _, _, size, month, day, year_or_time, name) = entry.split(/\s+/, 9)

        date = DateTime.parse("#{month} #{day}, #{year_or_time}")
        date = date.prev_year if date < DateTime.now

        @files[name] = {
          :size => size.to_i,
          :date => date
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
    groups = (1..@tpsize).map { Group.new }
    files_to_extract.sort { |a, b| b.last[:size] <=> a.last[:size] }.each do |name, props|

      # If the previous batch of files had an entry with the same size and date,
      # assume that this is a duplicate and assign it zero weight.  There may be
      # some false positives here since the granularity of the data we have from
      # tarsnap is only "same day".  However, a false positive just affects the
      # queing scheme, not which files get queued.

      size = (@options[:previous] && @options[:previous][name] == props) ? 0 : props[:size]
      groups.sort.last.add(name, size)
    end
    groups.map(&:files).reject(&:empty?)
  end

  def run
    file_groups.each_with_index do |chunk, idx|
      @work_qs[idx].push chunk
    end

    @thread_pool.map(&:join)
    @@output_mutex.synchronize { warn EXIT_ERROR } if @error
  end
end
