require 'csv'

module MapFields
  VERSION = '1.0.0'

  def self.included(base)
    base.extend(ClassMethods)
  end

  def map_fields
    default_options = {
      :file_field => 'file',
      :params => []
    }
    options = default_options.merge(
                self.map_fields_options
              )

    ::Rails.logger.debug("session[:map_fields]: #{session[:map_fields]}")
    ::Rails.logger.debug("params[options[:file_field]]: #{params[options[:file_field]]}")
    if session[:map_fields].nil? || !params[options[:file_field]].blank?
      session[:map_fields] = {}
      if params[options[:file_field]].blank?
        @map_fields_error = MissingFileContentsError
        return
      end

      file_field = params[options[:file_field]]
      file_name = "csv_#{Account.current.id}/#{Time.now.to_i}/#{file_field.original_filename}"

      AwsWrapper::S3Object.store(
            file_name,
            file_field.tempfile,
            S3_CONFIG[:bucket],
            :content_type => file_field.content_type
      )
      session[:map_fields][:file] = file_name
    else
      if session[:map_fields][:file].nil? || params[:fields].nil?
        session[:map_fields] = nil
        @map_fields_error =  InconsistentStateError
      else
        expected_fields = self.map_fields_fields
        if expected_fields.respond_to?(:call)
          expected_fields = expected_fields.call(params)
        end
        csv_file = AwsWrapper::S3Object.find(session[:map_fields][:file], S3_CONFIG[:bucket])
        @mapped_fields = []
        begin
          CSVBridge.parse(content_of(csv_file)) do |row|
             @mapped_fields << row
          end
        rescue CSVBridge::MalformedCSVError => e
          @map_fields_error = e
        end
      end
    end

    unless @map_fields_error
      @rows = []
      begin
        csv_file = AwsWrapper::S3Object.find(session[:map_fields][:file], S3_CONFIG[:bucket])
        CSVBridge.parse(content_of(csv_file)) do |row|
           @rows << row
           break if @rows.size == 2
        end
      rescue CSVBridge::MalformedCSVError => e
        @map_fields_error = e
      end
      expected_fields = self.map_fields_fields
      if expected_fields.respond_to?(:call)
        expected_fields = expected_fields.call(params)
      end
      @fields = (expected_fields).inject([]){ |o, e| o << [e, o.size]}
      @parameters = []
      options[:params].each do |param|
        @parameters += ParamsParser.parse(params, param)
      end
    end
  end

  def mapped_fields
    @mapped_fields
  end

  def content_of csv_file
    csv_file.read.force_encoding('utf-8').encode('utf-16', :undef => :replace, :invalid => :replace, :replace => '').encode('utf-8')
  end

  def fields_mapped?
    raise @map_fields_error if @map_fields_error
    @mapped_fields
  end

  def map_field_parameters(&block)

  end

  def map_fields_cleanup
    if @mapped_fields
      if session[:map_fields][:file]
        AwsWrapper::S3Object.delete(session[:map_fields][:file], S3_CONFIG[:bucket])
      end
      session[:map_fields] = nil
      @mapped_fields = nil
      @map_fields_error = nil
    end
  end

  module ClassMethods
    def map_fields(actions, fields, options = {})
      class_attribute :map_fields_fields, :instance_writer => false
      class_attribute :map_fields_options, :instance_writer => false
      self.map_fields_fields = fields
      self.map_fields_options = options
      before_filter :map_fields, :only => actions
      after_filter :map_fields_cleanup, :only => actions
    end
  end

  class MappedFields
    attr_reader :mapping, :ignore_first_row, :file

    def initialize(file, fields, mapping, ignore_first_row)
      @file = file
      @fields = fields
      @mapping = {}
      @ignore_first_row = ignore_first_row

      mapping.each do |k,v|
        #unless v.to_i == 0
          #Numeric mapping
          @mapping[v.to_i - 1] = k.to_i - 1
          #Text mapping
          @mapping[fields[v.to_i-1]] = k.to_i - 1
          #Symbol mapping
          sym_key = fields[v.to_i-1].downcase.
                                      gsub(/[-\s\/]+/, '_').
                                      gsub(/[^a-zA-Z0-9_]+/, '').
                                      to_sym
          @mapping[sym_key] = k.to_i - 1
        #end
      end
    end

    def is_mapped?(field)
      !@mapping[field].nil?
    end

    def each
      row_number = 1
      CSVBridge.foreach(@file) do |csv_row|
        unless row_number == 1 && @ignore_first_row
          row = {}
          @mapping.each do |k,v|
            row[k] = csv_row[v]
          end
          row.class.send(:define_method, :number) { row_number }
          yield(row)
        end
        row_number += 1
      end
    end
  end

  class InconsistentStateError < StandardError
  end

  class MissingFileContentsError < StandardError
  end

  class ParamsParser
    def self.parse(params, field = nil)
      result = []
      params.each do |key,value|
        if field.nil? || field.to_s == key.to_s
          check_values(value) do |k,v|
            result << ["#{key.to_s}#{k}", v]
          end
        end
      end
      result
    end

    private
      def self.check_values(value, &block)
        result = []
        if value.kind_of?(Hash)
          value.each do |k,v|
            check_values(v) do |k2,v2|
              result << ["[#{k.to_s}]#{k2}", v2]
            end
          end
        elsif value.kind_of?(Array)
          value.each do |v|
            check_values(v) do |k2, v2|
              result << ["[]#{k2}", v2]
            end
          end
        else
          result << ["", value]
        end
        result.each do |arr|
          yield arr[0], arr[1]
        end
      end
    end
end

if defined?(Rails) and defined?(ActionController)
  ActionController::Base.send(:include, MapFields)
  ActionController::Base.prepend_view_path File.expand_path(File.join(File.dirname(__FILE__), '..', 'views'))
end
