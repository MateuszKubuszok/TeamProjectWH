class Report
  attr_accessor :axes_count,
                :column_names,
                :column_full_names,
                :row_names,
                :row_full_names,
                :row_values

  def initialize result
    if result.respond_to? :key?
      @axes_count = result[:axes_count]
      @column_names = result[:column_names]
      @column_full_names = result[:column_full_names]
      @row_names = result[:row_names]
      @row_full_names = result[:row_full_names]
      @values = result[:values]
    else
      @axes_count = result.axes_count
      @column_names = result.column_names
      @column_full_names = result.column_full_names
      @row_names = result.row_names
      @row_full_names = result.row_full_names
      @values = result.values
    end
  end

  def to_html
    header = "\n\t\t<td></td>\n"
    @column_names.each { |name| header += "\t\t<td><b>#{name}</b></td>\n" }
    rows = "\n\t<tr>#{header}\t</tr>\n"
    @row_names.length.times do |i|
      row = "\t\t<td><b>#{@row_full_names[i]}</b></td>\n"
      @values[i].each { |value| row += "\t\t<td>#{value}</td>\n" }
      rows += "\t<tr>\n#{row}\t</tr>\n"
    end
    "<table>#{rows}</table>"
  end
end