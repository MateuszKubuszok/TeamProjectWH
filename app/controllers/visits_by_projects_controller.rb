class VisitsByProjectsController < ApplicationController
  def index
    @projects = ProjectCube.projects
  end

  def show
    begin
      @report = ProjectCube.visits_by_project(params).to_html.html_safe
    rescue NativeException
      @report = "Database error: #{$!}"
    end
  end
end
