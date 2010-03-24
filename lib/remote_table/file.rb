class RemoteTable
  class File
    attr_accessor :filename, :format, :delimiter, :skip, :cut, :crop, :sheet, :headers, :schema, :schema_name, :trap
    attr_accessor :encoding
    attr_accessor :path
    attr_accessor :keep_blank_rows
    
    def initialize(bus)
      @filename = bus[:filename]
      @format = bus[:format] || format_from_filename
      @delimiter = bus[:delimiter]
      @sheet = bus[:sheet] || 0
      @skip = bus[:skip] # rows
      @keep_blank_rows = bus[:keep_blank_rows] || false
      @crop = bus[:crop] # rows
      @cut = bus[:cut]   # columns
      @headers = bus[:headers]
      @schema = bus[:schema]
      @schema_name = bus[:schema_name]
      @trap = bus[:trap]
      @encoding = bus[:encoding] || 'UTF-8'
      extend "RemoteTable::#{format.to_s.camelcase}".constantize
    end
    
    class << self
      # http://santanatechnotes.blogspot.com/2005/12/matching-iso-8859-1-strings-with-ruby.html
      def convert_to_utf8(str, encoding)
        if encoding == 'UTF-8'
          str.toutf8 # just in case
        else
          @_iconv ||= Hash.new
          @_iconv[encoding] ||= Iconv.new 'UTF-8', encoding
          @_iconv[encoding].iconv(str).toutf8
        end
      end
    end
    
    def tabulate(path)
      define_fixed_width_schema! if format == :fixed_width and schema.is_a?(Array) # TODO move to generic subclass callback
      self.path = path
      self
    end
    
    private
    
    # doesn't support trap
    def define_fixed_width_schema!
      raise "can't define both schema_name and schema" if !schema_name.blank?
      self.schema_name = "autogenerated_#{filename.gsub(/[^a-z0-9_]/i, '')}".to_sym
      self.trap ||= lambda { true }
      Slither.define schema_name do |d|
        d.rows do |row|
          row.trap(&trap)
          schema.each do |name, width, options|
            if name == 'spacer'
              row.spacer width
            else
              row.column name, width, options
            end
          end
        end
      end
    end
    
    def backup_file!
      FileUtils.cp path, "#{path}.backup"
    end
    
    def skip_rows!
      return unless skip
      `cat #{path} | tail -n +#{skip + 1} > #{path}.tmp`
      FileUtils.mv "#{path}.tmp", path
    end
    
    def convert_file_to_utf8!
      return if encoding == 'UTF8' or encoding == 'UTF-8'
      `iconv -c -f #{encoding} -t UTF8 #{path} > #{path}.tmp`
      FileUtils.mv "#{path}.tmp", path
    end
    
    def restore_file!
      FileUtils.mv "#{path}.backup", path if ::File.readable? "#{path}.backup"
    end
    
    def cut_columns!
      return unless cut
      `cat #{path} | cut -c #{cut} > #{path}.tmp`
      FileUtils.mv "#{path}.tmp", path
    end
    
    def crop_rows!
      return unless crop
      `cat #{path} | tail -n +#{crop.first} | head -n #{crop.last - crop.first + 1} > #{path}.tmp`
      FileUtils.mv "#{path}.tmp", path
    end
    
    def format_from_filename
      extname = ::File.extname(filename).gsub('.', '')
      return :csv if extname.blank?
      format = [ :xls, :ods ].detect { |i| i == extname.to_sym }
      format = :csv if format.blank?
      format
    end
  end
end
