class UserProjectsOnDatesController < ApplicationController
  def index
    @users = TicketCube.users
  end

  def show
    begin
      @report = TicketCube.pmt_project_user_date(params).to_html.html_safe
    rescue NativeException
      @report = "Database error: #{$!}"
    end
  end
end
