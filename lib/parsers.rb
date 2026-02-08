require 'date'

module Parsers
  module_function

  def parse_datetime(datetime_str, _logger = nil, _field_name = 'datetime')
    return nil if datetime_str.to_s.strip.empty?
    DateTime.parse(datetime_str.to_s)
  rescue StandardError
    nil
  end

  def parse_date(date_str, _logger = nil, _field_name = 'date')
    return nil if date_str.to_s.strip.empty?
    DateTime.parse(date_str.to_s).to_date
  rescue StandardError
    nil
  end

  def parse_float(float_str, _logger = nil, _field_name = 'float')
    return nil if float_str.to_s.strip.empty?
    Float(float_str.to_s.gsub(/[$,]/, ''))
  rescue StandardError
    nil
  end

  def parse_boolean(bool_str, _logger = nil, _field_name = 'boolean')
    return nil if bool_str.to_s.strip.empty?
    val = bool_str.to_s.downcase
    return true if %w[true t yes y 1].include?(val)
    return false if %w[false f no n 0].include?(val)
    nil
  end
end
