class RemoteTable
  # Mixed in to process XML and XHTML.
  module ProcessedByNokogiri
    WHITESPACE = /\s+/
    SINGLE_SPACE = ' '
    SOFT_HYPHEN = '&shy;'

    # Yield each row using Nokogiri.
    def _each
      require 'nokogiri'
      require 'cgi'
      
      # save this to a local var because we modify it in the loop
      current_headers = headers

      unless row_css or row_xpath
        raise ::ArgumentError, "[remote_table] Need :row_css or :row_xpath in order to process XML or HTML"
      end

      #delete_harmful!
      #transliterate_whole_file_to_utf8!

      rows_processed = 0

      
      xml = nokogiri_class.parse(unescaped_xml_without_soft_hyphens, nil, RemoteTable::EXTERNAL_ENCODING)
      (row_css ? xml.css(row_css) : xml.xpath(row_xpath)).each do |row|
        rows_processed += 1
        if skip > 0 && rows_processed <= skip
          puts "skiping row #{rows_processed}."
        else
          some_value_present = false
          values = if column_css
            row.css column_css
          elsif column_xpath
            row.xpath column_xpath
          else
            [row]
          end.map do |cell|
            memo = cell.content.dup
            memo = assume_utf8 memo
            memo.gsub! WHITESPACE, SINGLE_SPACE
            memo.strip!
            if not some_value_present and not keep_blank_rows and memo.present?
              some_value_present = true
            end
            memo
          end
          if current_headers == :first_row
            current_headers = values.select(&:present?)
            next
          end
          if keep_blank_rows or some_value_present
            if not headers
              yield values
            else
              yield zip(current_headers, values)
            end
          end
        end
      end
    ensure
      local_copy.cleanup
    end

    private
    
    # http://snippets.dzone.com/posts/show/406
    def zip(keys, values)
      hash = ::ActiveSupport::OrderedHash.new
      keys.zip(values) { |k,v| hash[k]=v }
      hash
    end

    # should we be doing this in ruby?
    def unescaped_xml_without_soft_hyphens
      str = ::CGI.unescapeHTML local_copy.encoded_io.read
      local_copy.encoded_io.rewind
      # get rid of MS Office baddies
      str.gsub! SOFT_HYPHEN, ''
      str
    end
  end
end
