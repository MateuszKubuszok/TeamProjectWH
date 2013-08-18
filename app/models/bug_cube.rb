class BugCube < Warehouse
  @@schema = Mondrian::OLAP::Schema.define 'TeamProject BugCube' do
    cube 'Bug Cube' do
      table 'bug_facts'

      dimension 'Date', foreign_key: 'date_id', type: 'TimeDimension' do
        hierarchy 'Dates', has_all: false, primary_key: 'id' do
          table 'date_dimension'
          level 'Years',    column: 'calendar_year',                type: 'String',   unique_members: false, level_type: 'TimeYears'
          level 'Months',   column: 'calendar_month_number',        type: 'Numeric',  unique_members: false, level_type: 'TimeMonths', name_column: 'calendar_month_name'
          level 'Days',     column: 'day_number_in_calendar_month', type: 'Numeric',  unique_members: false, level_type: 'TimeDays'
        end
      end

      dimension 'Bugs', foreign_key: 'date_bug' do
        hierarchy 'Projects', has_all: true, all_member_name: 'All Projects', primary_key: 'date_bug' do
          table 'bug_dimension'
          level 'Projects', column: 'project_id',                   type: 'Numeric', unique_members: false, name_column: 'project_name'
          level 'Types',    column: 'type_id',                      type: 'Numeric', unique_members: false, name_column: 'type_name'
        end
        hierarchy 'Resolved', has_all: true, all_member_name: 'All Bugs', primary_key: 'date_bug' do
          table 'bug_dimension'
          level 'Resolved', column: 'resolved',                     type: 'Boolean', unique_members: true
        end
        hierarchy 'Type', has_all: true, all_member_name: 'All Types', primary_key: 'date_bug' do
          table 'bug_dimension'
          level 'Types',    column: 'type_id',                      type: 'Numeric', unique_members: false, name_column: 'type_name'
        end
      end

      measure 'Bug Count', column: 'bug_id', type: 'Numeric', aggregator: :count
    end
  end

  def self.dates
    result = ActiveRecord::Base.connection.execute <<-SQL
      SELECT DISTINCT
        date_dimension.calendar_year AS year,
        date_dimension.calendar_month_name AS month,
        date_dimension.day_number_in_calendar_month AS day
      FROM
        public.bug_facts,
        public.date_dimension,
        public.bug_dimension
      WHERE
        date_dimension.id = bug_facts.date_id AND
        bug_dimension.date_bug = bug_facts.date_bug
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
        bug_dimension.project_name AS name
      FROM
        public.bug_facts,
        public.date_dimension,
        public.bug_dimension
      WHERE
        date_dimension.id = bug_facts.date_id AND
        bug_dimension.date_bug = bug_facts.date_bug
      ORDER BY
        name;
    SQL
    projects = []
    result.each { |r| projects << r.symbolize_keys[:name].gsub('/','_') }
    projects
  end

  def self.bug_completeness params = {}
    date = parse_time params

    all = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        ORDER(
          {[Bugs.Projects].MEMBERS},
          [Bugs.Projects].CURRENTMEMBER.Name,
          ASC
        ) ON ROWS
      FROM [Bug Cube]
      WHERE ([Date.Dates].#{date})
    MDX

    completed = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        ORDER(
          {[Bugs.Projects].MEMBERS},
          [Bugs.Projects].CURRENTMEMBER.Name,
          ASC
        ) ON ROWS
      FROM [Bug Cube]
      WHERE ([Date.Dates].#{date}, [Bugs.Resolved].[true])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[Bugs\.Projects\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0]
      else
        completed_value = completed.values[i][0]
        completed_value = 0 if completed_value.blank?
        rows.append [all_value, completed_value, completed_value.to_f/all_value.to_f*100.0]
      end
    end

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Completed Completion',
      column_full_names:  %W'All Completed Completion',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end

  def self.bug_projects params = {}
    dates = self.dates
    from = parse_time dates[0]
    to = parse_time dates[-1]
    name = params[:id].gsub('_','/')

    all = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        {DESCENDANTS(
          [Date.Dates].#{from}:[Date.Dates].#{to},
          [Date.Dates].[Days]
        )} ON ROWS
      FROM [Bug Cube]
      WHERE ([Bugs.Projects].[#{name}],[Bugs.Resolved].[false])
    MDX

    begin
      bug = self.mdx <<-MDX
        SELECT
          {[Measures].[Bug Count]} ON COLUMNS,
          {DESCENDANTS(
            [Date.Dates].#{from}:[Date.Dates].#{to},
            [Date.Dates].[Days]
          )} ON ROWS
        FROM [Bug Cube]
        WHERE ([Bugs.Projects].[#{name}].[bug],[Bugs.Resolved].[false])
      MDX
    rescue NativeException
      bug = nil
    end

    begin
      enhancement = self.mdx <<-MDX
        SELECT
          {[Measures].[Bug Count]} ON COLUMNS,
          {DESCENDANTS(
            [Date.Dates].#{from}:[Date.Dates].#{to},
            [Date.Dates].[Days]
          )} ON ROWS
        FROM [Bug Cube]
        WHERE ([Bugs.Projects].[#{name}].[enhancement],[Bugs.Resolved].[false])
      MDX
    rescue NativeException
      enhancement = nil
    end

    begin
      fatal_error = self.mdx <<-MDX
        SELECT
          {[Measures].[Bug Count]} ON COLUMNS,
          {DESCENDANTS(
            [Date.Dates].#{from}:[Date.Dates].#{to},
            [Date.Dates].[Days]
          )} ON ROWS
        FROM [Bug Cube]
        WHERE ([Bugs.Projects].[#{name}].[fatal_error],[Bugs.Resolved].[false])
      MDX
    rescue NativeException
      fatal_error = nil
    end

    begin
      feature = self.mdx <<-MDX
        SELECT
          {[Measures].[Bug Count]} ON COLUMNS,
          {DESCENDANTS(
            [Date.Dates].#{from}:[Date.Dates].#{to},
            [Date.Dates].[Days]
          )} ON ROWS
        FROM [Bug Cube]
        WHERE ([Bugs.Projects].[#{name}].[feature],[Bugs.Resolved].[false])
      MDX
    rescue NativeException
      feature = nil
    end

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[Date\.Dates\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0, 0, 0]
      else
        all_value = all.values[i][0]
        bug_value = bug.nil? ? 0 : bug.values[i][0]
        enhancement_value = enhancement.nil? ? 0 : enhancement.values[i][0]
        fatal_error_value = fatal_error ? 0 : fatal_error.values[i][0]
        feature_value = all_value.to_s.to_f - bug_value.to_s.to_f - fatal_error.to_s.to_f
        rows.append [all_value, bug_value, enhancement_value, fatal_error_value, feature_value]
      end
    end

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Bug Enhancement FatalError Feature',
      column_full_names:  %W'All Bug Enhancement FatalError Feature',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end

  def self.bug_types params={}
    date = parse_time params

    all = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        {DESCENDANTS(
          [Bugs.Projects].MEMBERS,
          [Bugs.Projects].[Projects]
        )} ON ROWS
      FROM [Bug Cube]
      WHERE ([Date.Dates].#{date})
    MDX

    bug = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        {DESCENDANTS(
          [Bugs.Projects].MEMBERS,
          [Bugs.Projects].[Projects]
        )} ON ROWS
      FROM [Bug Cube]
      WHERE ([Date.Dates].#{date}, [Bugs.Type].[bug])
    MDX

    enhancement = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        {DESCENDANTS(
          [Bugs.Projects].MEMBERS,
          [Bugs.Projects].[Projects]
        )} ON ROWS
      FROM [Bug Cube]
      WHERE ([Date.Dates].#{date}, [Bugs.Type].[enhancement])
    MDX

    fatal_error = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        {DESCENDANTS(
          [Bugs.Projects].MEMBERS,
          [Bugs.Projects].[Projects]
        )} ON ROWS
      FROM [Bug Cube]
      WHERE ([Date.Dates].#{date}, [Bugs.Type].[fatal_error])
    MDX

    feature = self.mdx <<-MDX
      SELECT
        {[Measures].[Bug Count]} ON COLUMNS,
        {DESCENDANTS(
          [Bugs.Projects].MEMBERS,
          [Bugs.Projects].[Projects]
        )} ON ROWS
      FROM [Bug Cube]
      WHERE ([Date.Dates].#{date}, [Bugs.Type].[feature])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[Bugs\.Projects\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0]
      else
        all_value = all.values[i][0].to_i
        bug_value = bug.values[i][0].to_i
        enhancement_value = enhancement.values[i][0].to_i
        fatal_error_value = fatal_error.values[i][0].to_i
        feature_value = feature.values[i][0].to_i
        rows.append [all_value, bug_value, enhancement_value, fatal_error_value, feature_value]
      end
    end

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Bug Enhancement FatalError Feature',
      column_full_names:  %W'All Bug Enhancement FatalError Feature',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end
end