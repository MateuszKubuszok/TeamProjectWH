class BugTypesController < ApplicationController
  def index
    @dates = BugCube.dates
  end

  def show
    begin
      @report = BugCube.bug_types(params).to_html.html_safe
    rescue NativeException
      @report = "Database error: #{$!}"
    end
  end
end
