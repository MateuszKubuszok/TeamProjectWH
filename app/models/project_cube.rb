class ProjectCube < Warehouse
  @@schema = Mondrian::OLAP::Schema.define 'TeamProject ProjectCube' do
    cube 'Project Cube' do
      table 'project_facts'

      dimension 'Date', foreign_key: 'date_id', type: 'TimeDimension' do
        hierarchy 'Dates', has_all: false, primary_key: 'id' do
          table 'date_dimension'
          level 'Years',    column: 'calendar_year',                type: 'String',   unique_members: false, level_type: 'TimeYears'
          level 'Months',   column: 'calendar_month_number',        type: 'Numeric',  unique_members: false, level_type: 'TimeMonths', name_column: 'calendar_month_name'
          level 'Days',     column: 'day_number_in_calendar_month', type: 'Numeric',  unique_members: false, level_type: 'TimeDays'
        end
      end

      dimension 'Projects', foreign_key: 'date_project' do
        hierarchy 'Projects', has_all: true, all_member_name: 'All Visits', primary_key: 'date_project' do
          table 'project_dimension'
          level 'Projects',  column: 'project_id',                  type: 'Numeric',  unique_members: true, name_column: 'name'
        end
      end

      measure 'Project Count', column: 'visits', aggregator: :max
    end
  end

  def self.dates
    result = ActiveRecord::Base.connection.execute <<-SQL
      SELECT DISTINCT
        date_dimension.calendar_year AS year,
        date_dimension.calendar_month_name AS month,
        date_dimension.day_number_in_calendar_month AS day
      FROM
        public.project_facts,
        public.date_dimension,
        public.project_dimension
      WHERE
        date_dimension.id = project_facts.date_id AND
        project_dimension.date_project = project_facts.date_project
      ORDER BY
        year, month, day;
    SQL
    dates = []
    result.each do |r|
      r = r.symbolize_keys
      dates << { year: r[:year], month: r[:month], day: r[:day] }
    end
    dates
  end

  def self.projects
    result = ActiveRecord::Base.connection.execute <<-SQL
      SELECT DISTINCT
        project_dimension.name AS name
      FROM
        public.project_facts,
        public.date_dimension,
        public.project_dimension
      WHERE
        date_dimension.id = project_facts.date_id AND
        project_dimension.date_project = project_facts.date_project
      ORDER BY
        name;
    SQL
    projects = []
    result.each { |r| projects << r.symbolize_keys[:name].gsub('/','_') }
    projects
  end

  def self.visits_by_date params = {}
    date = parse_time params

    all = self.mdx <<-MDX
      SELECT
        {[Measures].[Project Count]} ON COLUMNS,
        ORDER(
          {DESCENDANTS(
            [Projects.Projects].CHILDREN,
            [Projects.Projects].[Projects]
          )},
          [Measures].[Project Count],
          DESC
        ) ON ROWS
      FROM [Project Cube]
      WHERE ([Date.Dates].#{date})
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[Projects\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'Visits',
      column_full_names:  %W'Visits',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             all.values
    )
  end

  def self.visits_by_project params = {}
    dates = self.dates
    from = parse_time dates[0]
    to = parse_time dates[-1]
    project = params[:project].gsub('_','/')

    all = self.mdx <<-MDX
      SELECT
        {[Measures].[Project Count]} ON COLUMNS,
        {[Date.Dates].#{from}:[Date.Dates].#{to}} ON ROWS
      FROM [Project Cube]
      WHERE ([Projects.Projects].[#{project}])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[Date\.Dates\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      if all_value.blank? || all_value==0
        rows.append [0, 0]
      else
        increased_value = all.values[i][0].to_s.to_f - all.values[i-1][0].to_s.to_f
        rows.append [all_value, increased_value]
      end
    end
    rows[0] = [ all.values[0][0], all.values[0][0] ] if all.values.length > 0

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'Visits Increased',
      column_full_names:  %W'Visits Increased',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end
end