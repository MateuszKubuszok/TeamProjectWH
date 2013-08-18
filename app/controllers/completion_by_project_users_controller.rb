class CompletionByProjectUsersController < ApplicationController
  def index
    @projects = TicketCube.projects
  end

  def show
    begin
      @report = TicketCube.pmt_project_user(params).to_html.html_safe
    rescue NativeException
      @report = "Database error: #{$!}"
    end
  end
end
