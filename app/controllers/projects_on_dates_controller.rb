class ProjectsOnDatesController < ApplicationController
  def index
    @dates = TicketCube.dates
  end

  def show
    begin
      @report = TicketCube.pmt_date(params).to_html.html_safe
    rescue NativeException
      @report = "Database error: #{$!}"
    end
  end
end
