class VisitsByDateController < ApplicationController
  def index
    @dates = ProjectCube.dates
  end

  def show
    begin
      @report = ProjectCube.visits_by_date(params).to_html.html_safe
    rescue NativeException
      @report = "Database error: #{$!}"
    end
  end
end
