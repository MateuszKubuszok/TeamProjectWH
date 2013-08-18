class BugProjectsController < ApplicationController
  def index
    @projects = BugCube.projects
  end

  def show
    begin
      @report = BugCube.bug_projects(params).to_html.html_safe
    rescue NativeException
      @report = "Database error: #{$!}"
    end
  end
end
