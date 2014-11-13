# encoding: utf-8
require 'helper'
require 'tempfile'

describe RemoteTable do
  describe 'used on remote files' do
    it "add a row hash to every row" do
      t = RemoteTable.new(:url => 'http://www.customerreferenceprogram.org/uploads/CRP_RFP_template.xlsx')
      t[5].row_hash.must_equal "06d8a738551c17735e2731e25c8d0461"
    end

    it "open a google doc" do
      t = RemoteTable.new 'http://spreadsheets.google.com/pub?key=tObVAGyqOkCBtGid0tJUZrw'
      t[0]['name'].must_equal 'Seamus Abshere'
    end

    it "open a csv with custom headers" do
      t = RemoteTable.new 'http://spreadsheets.google.com/pub?key=tObVAGyqOkCBtGid0tJUZrw', :headers => %w{ col1 col2 col3 }
      t[0]['col2'].must_equal 'name'
      t[1]['col2'].must_equal 'Seamus Abshere'
    end

    it "return an ordered hash" do
      t = RemoteTable.new 'http://spreadsheets.google.com/pub?key=tObVAGyqOkCBtGid0tJUZrw'
      t[0].class.must_equal ::ActiveSupport::OrderedHash
    end

    it "open a csv inside a zip file" do
      t = RemoteTable.new('http://www.epa.gov/climatechange/Downloads/ghgemissions/2011-Annex-Tables.zip',
            :filename => 'Annex Tables/Table A-93.csv',
            :skip => 1,
            :headers => %w{ age LDGV LDGT HDGV LDDV LDDT HDDT MC },
            :select => proc { |row| row['age'].to_i.to_s == row['age'] })
      t[0]['LDGV'].must_equal '5.20%'
    end

    it 'not blow up if each is called twice' do
      t = RemoteTable.new 'http://spreadsheets.google.com/pub?key=tObVAGyqOkCBtGid0tJUZrw'
      count = 0
      t.each { |row| count += 1 }
      first_run = count
      t.each { |row| count += 1}
      count.must_equal first_run*2
    end

    it 'allow itself to be cleared for save memory' do
      t = RemoteTable.new 'http://spreadsheets.google.com/pub?key=tObVAGyqOkCBtGid0tJUZrw'
      t.to_a
      t.send(:cache).length.must_be :>, 0
      t.free
      t.send(:cache).length.must_equal 0
    end

    it %{transliterate characters from ISO-8859-1} do
      t = RemoteTable.new :url => 'http://static.brighterplanet.com/science/data/consumables/pets/breed_genders.csv', :encoding => 'ISO-8859-1'
      t.rows.detect { |row| row['name'] == 'Briquet Griffon Vendéen' }.wont_equal nil
    end

    it %{read xml with css selectors} do
      t = RemoteTable.new 'http://www.nanonull.com/TimeService/TimeService.asmx/getCityTime?city=Chicago', :format => :xml, :row_css => 'string', :headers => false
      /(AM|PM)/.match(t[0][0]).wont_equal nil
    end

    it %{optionally stream rows instead of caching them} do
      t = RemoteTable.new 'http://www.earthtools.org/timezone/40.71417/-74.00639', :format => :xml, :row_xpath => '//timezone/isotime', :headers => false, :streaming => true
      time1 = t[0][0]
      /\d\d\d\d-\d\d-\d\d/.match(time1).wont_equal nil
      sleep 1
      time2 = t[0][0]
      time1.wont_equal time2
    end

    it %{reads country names} do
      t = RemoteTable.new 'http://www.iso.org/iso/country_names_and_code_elements_txt', :skip => 1, :headers => false, :delimiter => ';'
      t[1][0].must_equal %{ÅLAND ISLANDS}
    end

    it %{parse a big CSV that is not UTF-8} do
      t = RemoteTable.new 'https://openflights.svn.sourceforge.net/svnroot/openflights/openflights/data/airports.dat', :headers => false#, :encoding => 'UTF-8'
      t[0][1].must_equal 'Goroka'
    end

    it "read only certain rows of an XLSX" do
      t = RemoteTable.new 'test/data/backup/http___www.customerreferenceprogram.org_uploads_CRP_RFP_template.xlsx', :crop => 11..16, :headers => false
      t[0][0].must_equal "Permissioning and access groups for all content"
      t[4][0].must_equal "Manage Multiple Incentive Programs for Participants"

      t = RemoteTable.new 'test/data/backup/http___www.customerreferenceprogram.org_uploads_CRP_RFP_template.xlsx', :crop => 11..16, :headers => %w{ col1 }
      t[0]['col1'].must_equal "Permissioning and access groups for all content"
      t[4]['col1'].must_equal "Manage Multiple Incentive Programs for Participants"
    end

    it "doesn't get confused by :format => nil" do
      t = RemoteTable.new :url => 'http://www.fueleconomy.gov/FEG/epadata/00data.zip', :filename => 'G6080900.xls', :format => nil
      t[0]['Class'].must_equal 'TWO SEATERS'
    end
  end
end
