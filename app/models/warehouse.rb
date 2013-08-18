require 'rubygems'
require 'mondrian-olap'
require 'jdbc/postgres'

class Warehouse
  @@schema = Mondrian::OLAP::Schema.define 'TeamProject Warehouse'

  def self.schema
    @@schema
  end

  def self.connection
    return @@connection if defined?(@@connection)

    config = Rails.configuration.database_configuration[Rails.env].symbolize_keys

    @@connection = Mondrian::OLAP::Connection.create(
      driver:   config[:adapter],
      host:     config[:host],
      database: config[:database],
      username: config[:username],
      password: config[:password],
      schema:   schema
    )
  end

  def self.mdx query
    puts "MDX: #{query}\n----\n"
    result = connection.execute query
    puts "Success!"
    result
  end

  def self.parse_time params = {}
    params = params.symbolize_keys
    return "[#{Date.current.year}]" if params.blank? || !params.key?(:year)
    result = "[#{params[:year]}]"
    if params.key?(:month)
      result += ".[#{params[:month]}]"
      result += ".[#{params[:day]}]" if params.key?(:day)
    end
    result
  end
end