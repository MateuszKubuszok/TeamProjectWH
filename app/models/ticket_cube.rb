class TicketCube < Warehouse
  @@schema = Mondrian::OLAP::Schema.define 'TeamProject TicketCube' do
    cube 'Ticket Cube' do
      table 'ticket_facts'

      dimension 'Date', foreign_key: 'date_id', type: 'TimeDimension' do
        hierarchy 'Dates', has_all: false, primary_key: 'id' do
          table 'date_dimension'
          level 'Years',    column: 'calendar_year',                type: 'String',   unique_members: false, level_type: 'TimeYears'
          level 'Months',   column: 'calendar_month_number',        type: 'Numeric',  unique_members: false, level_type: 'TimeMonths', name_column: 'calendar_month_name'
          level 'Days',     column: 'day_number_in_calendar_month', type: 'Numeric',  unique_members: false, level_type: 'TimeDays'
        end
      end

      dimension 'PMT', foreign_key: 'date_pmt' do
        hierarchy 'PMT', has_all: true, all_member_name: 'All PMT', primary_key: 'date_pmt' do
          table 'pmt_dimension'
          level 'Projects',  column: 'project',                      type: 'Numeric',  unique_members: false, name_column: 'project_name'
          level 'Milestones',column: 'milestone',                    type: 'Numeric',  unique_members: false, name_column: 'milestone_name'
          level 'Tickets',   column: 'ticket',                       type: 'Numeric',  unique_members: false, name_column: 'ticket_name'
        end
        hierarchy 'Resolved', has_all: true, all_member_name: 'All Tickets', primary_key: 'date_pmt' do
          table 'pmt_dimension'
          level 'Resolved', column: 'completed',                    type: 'Boolean',  unique_members: false
        end
      end

      dimension 'Users', foreign_key: 'date_user' do
        hierarchy 'Users', has_all: true, all_member_name: 'All Users', primary_key: 'date_user' do
          table 'user_dimension'
          level 'Users',    column: 'login',                    type: 'String',  unique_members: false
        end
      end

      measure 'Ticket Count',     column: 'ticket_id',      aggregator: :count
      measure 'Average Deadline', column: 'till_deadline',  aggregator: :avg
    end
  end

  def self.dates
    result = ActiveRecord::Base.connection.execute <<-SQL
      SELECT DISTINCT
        date_dimension.calendar_year AS year,
        date_dimension.calendar_month_name AS month,
        date_dimension.day_number_in_calendar_month AS day
      FROM
        public.ticket_facts,
        public.date_dimension,
        public.pmt_dimension,
        public.user_dimension
      WHERE
        date_dimension.id = ticket_facts.date_id AND
        user_dimension.date_user = ticket_facts.date_user AND
        pmt_dimension.date_pmt = ticket_facts.date_pmt
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

  def self.dates_for_user user
    result = ActiveRecord::Base.connection.execute <<-SQL
      SELECT DISTINCT
        date_dimension.calendar_year AS year,
        date_dimension.calendar_month_name AS month,
        date_dimension.day_number_in_calendar_month AS day
      FROM
        public.ticket_facts,
        public.date_dimension,
        public.pmt_dimension,
        public.user_dimension
      WHERE
        date_dimension.id = ticket_facts.date_id AND
        user_dimension.date_user = ticket_facts.date_user AND
        pmt_dimension.date_pmt = ticket_facts.date_pmt AND
        user_dimension.login = '#{user}'
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
        pmt_dimension.project_name AS name
      FROM
        public.ticket_facts,
        public.date_dimension,
        public.pmt_dimension,
        public.user_dimension
      WHERE
        date_dimension.id = ticket_facts.date_id AND
        pmt_dimension.date_pmt = ticket_facts.date_pmt AND
        user_dimension.date_user = ticket_facts.date_user
      ORDER BY
        name;
    SQL
    projects = []
    result.each { |r| projects << r.symbolize_keys[:name].gsub('/','_') }
    projects
  end

  def self.users
    result = ActiveRecord::Base.connection.execute <<-SQL
      SELECT DISTINCT
        user_dimension.login AS login
      FROM
        public.ticket_facts,
        public.date_dimension,
        public.pmt_dimension,
        public.user_dimension
      WHERE
        date_dimension.id = ticket_facts.date_id AND
        pmt_dimension.date_pmt = ticket_facts.date_pmt AND
        user_dimension.date_user = ticket_facts.date_user
      ORDER BY
        login;
    SQL
    users = []
    result.each { |r| users << r.symbolize_keys[:login] }
    users
  end

  def self.users_for_project project
    result = ActiveRecord::Base.connection.execute <<-SQL
      SELECT DISTINCT
        user_dimension.login AS login
      FROM
        public.ticket_facts,
        public.date_dimension,
        public.pmt_dimension,
        public.user_dimension
      WHERE
        date_dimension.id = ticket_facts.date_id AND
        pmt_dimension.date_pmt = ticket_facts.date_pmt AND
        user_dimension.date_user = ticket_facts.date_user AND
        pmt_dimension.project_name = '#{project.gsub('_','/')}'
      ORDER BY
        login;
    SQL
    users = []
    result.each { |r| users << r.symbolize_keys[:login] }
    users
  end

  def self.pmt_date params = {}
    date = parse_time params

    # podawać datę
    all =  self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        NON EMPTY {DESCENDANTS(
          [PMT.PMT].MEMBERS,
          [PMT.PMT].[Projects]
        )} ON ROWS
      FROM [Ticket Cube]
      WHERE ([Date.Dates].#{date})
    MDX

    completed = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        NON EMPTY {DESCENDANTS(
          [PMT.PMT].MEMBERS,
          [PMT.PMT].[Projects]
        )} ON ROWS
      FROM [Ticket Cube]
      WHERE ([Date.Dates].#{date}, [PMT.Resolved].[true])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[PMT\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      dead_value = all.values[i][1]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0, 0]
      else
        completed_value = completed.values[i][0]
        completed_value = 0 if completed_value.blank?
        rows.append [all_value, completed_value, completed_value.to_f/all_value.to_f*100.0, dead_value]
      end
    end

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Completed Completion AvgTillDeadline',
      column_full_names:  %W'All Completed Completion AvgTillDeadline',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end

  def self.pmt_project params = {}
    dates = self.dates
    from = parse_time dates[0]
    to = parse_time dates[-1]
    project = params[:project].gsub('_','/')

    # podawać projekt i zakres
    all = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        {[Date.Dates].#{from}:[Date.Dates].#{to}} ON ROWS
      FROM [Ticket Cube]
      WHERE ([PMT.PMT].[#{project}])
    MDX

    completed = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        {[Date.Dates].#{from}:[Date.Dates].#{to}} ON ROWS
      FROM [Ticket Cube]
      WHERE ([PMT.PMT].[#{project}], [PMT.Resolved].[true])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[Date\.Dates\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      dead_value = all.values[i][1]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0, 0]
      else
        completed_value = completed.values[i][0]
        completed_value = 0 if completed_value.blank?
        rows.append [all_value, completed_value, completed_value.to_f/all_value.to_f*100.0, dead_value]
      end
    end

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Completed Completion AvgTillDeadline',
      column_full_names:  %W'All Completed Completion AvgTillDeadline',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end

  def self.pmt_project_user params = {}
    dates = self.dates
    from = parse_time dates[0]
    to = parse_time dates[-1]
    project = params[:project].gsub('_','/')
    user = params[:user]

    # podawać projekt, użytkownika i zakres
    all = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        {[Date.Dates].#{from}:[Date.Dates].#{to}} ON ROWS
      FROM [Ticket Cube]
      WHERE ([PMT.PMT].[#{project}], [Users.Users].[#{user}])
    MDX

    completed = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        {[Date.Dates].#{from}:[Date.Dates].#{to}} ON ROWS
      FROM [Ticket Cube]
      WHERE ([PMT.PMT].[#{project}], [Users.Users].[#{user}], [PMT.Resolved].[true])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[Date\.Dates\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      dead_value = all.values[i][1]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0, 0]
      else
        completed_value = completed.values[i][0]
        completed_value = 0 if completed_value.blank?
        rows.append [all_value, completed_value, completed_value.to_f/all_value.to_f*100.0, dead_value]
      end
    end

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Completed Completion AvgTillDeadline',
      column_full_names:  %W'All Completed Completion AvgTillDeadline',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end

  def self.pmt_project_user_date params = {}
    date = parse_time params
    user = params[:user]

    # podawać użytkownika i datę
    all = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        NON EMPTY {DESCENDANTS(
          [PMT.PMT].MEMBERS,
          [PMT.PMT].[Projects]
        )} ON ROWS
      FROM [Ticket Cube]
      WHERE ([Date.Dates].#{date}, [Users.Users].[#{user}])
    MDX

    completed = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        NON EMPTY {DESCENDANTS(
          [PMT.PMT].MEMBERS,
          [PMT.PMT].[Projects]
        )} ON ROWS
      FROM [Ticket Cube]
      WHERE ([Date.Dates].#{date}, [Users.Users].[#{user}], [PMT.Resolved].[true])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[PMT\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      dead_value = all.values[i][1]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0, dead_value]
      else
        completed_value = completed.values[i][0]
        completed_value = 0 if completed_value.blank?
        rows.append [all_value, completed_value, completed_value.to_f/all_value.to_f*100.0, dead_value]
      end
    end

    puts YAML.dump(rows)

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Completed Completion AvgTillDeadline',
      column_full_names:  %W'All Completed Completion AvgTillDeadline',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end

  def self.pmt_milestones_user_date params = {}
    date = parse_time params
    user = params[:user]

    # podawać użytkownika i datę
    all = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        {DESCENDANTS(
          [PMT.PMT].MEMBERS,
          [PMT.PMT].[Milestones]
        )} ON ROWS
      FROM [Ticket Cube]
      WHERE ([Date.Dates].#{date},[Users.Users].[#{user}])
    MDX

    completed = self.mdx <<-MDX
      SELECT
        {[Measures].MEMBERS} ON COLUMNS,
        {DESCENDANTS(
          [PMT.PMT].MEMBERS,
          [PMT.PMT].[Milestones]
        )} ON ROWS
      FROM [Ticket Cube]
      WHERE ([Date.Dates].#{date},[Users.Users].[#{user}], [PMT.Resolved].[true])
    MDX

    row_names = []
    all.row_full_names.length.times { |i| row_names.append all.row_full_names[i].gsub(/(\[PMT\]\.)|(\[)|(\])/, '').gsub('.', '/') }

    rows = []
    all.values.length.times do |i|
      all_value = all.values[i][0]
      dead_value = all.values[i][1]
      if all_value.blank? || all_value==0
        rows.append [0, 0, 0, dead_value]
      else
        completed_value = completed.values[i][0]
        completed_value = 0 if completed_value.blank?
        rows.append [all_value, completed_value, completed_value.to_f/all_value.to_f*100.0, dead_value]
      end
    end

    puts YAML.dump(rows)

    Report.new(
      axes_count:         all.axes_count,
      column_names:       %W'All Completed Completion AvgTillDeadline',
      column_full_names:  %W'All Completed Completion AvgTillDeadline',
      row_names:          row_names,
      row_full_names:     row_names,
      values:             rows
    )
  end
end