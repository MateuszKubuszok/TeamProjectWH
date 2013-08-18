class BugCompletionController < ApplicationController
  def index
    @dates = BugCube.dates
  end

  def show
    begin
      @report = BugCube.bug_completeness(params).to_html.html_safe
    rescue NativeException
      @report = 'There isn\'t such date in database'
    end
  end
end
