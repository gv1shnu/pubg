"""Idempotent dashboard authoring inside the pinned Superset application."""
import json
import os
from sqlalchemy import text
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import db, security_manager
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable, TableColumn

    admin = security_manager.find_user(username='admin')
    if admin is None:
        admin = security_manager.add_user('admin', 'Demo', 'Administrator', 'admin@example.invalid',
            security_manager.find_role('Admin'), os.environ['SUPERSET_ADMIN_PASSWORD'])
    database = db.session.query(Database).filter_by(database_name='Battleground PostgreSQL').one_or_none()
    if database is None:
        database = Database(database_name='Battleground PostgreSQL')
        db.session.add(database)
    database.sqlalchemy_uri = 'postgresql+psycopg2://tableau_reader:' + os.environ['TABLEAU_PASSWORD'] + '@postgres/telemetry'
    database.expose_in_sqllab = True
    database.allow_dml = False
    database.allow_run_async = False
    db.session.commit()

    queries = {
        'leaderboard': 'select *, case when completed_eligible_matches=1 then placement::numeric end as average_placement from mart.leaderboard where namespace=\'live\'',
        'match_overview': 'select * from mart.match_overview where namespace=\'live\'',
        'match_timeline': 'select * from mart.match_timeline where namespace=\'live\'',
        'server_sessions': 'select distinct namespace,run_id,server_id,region,current_online_players,current_connections from mart.server_health where namespace=\'live\'',
        'server_health': 'select * from mart.server_health where namespace=\'live\'',
        'pipeline_health': 'select sampled_at,metric,labels::text as labels,value from mart.pipeline_health',
        'pipeline_current': 'select distinct on(metric,labels) sampled_at,metric,labels::text as labels,value from mart.pipeline_health order by metric,labels,sampled_at desc',
        'freshness': 'select * from mart.freshness where namespace=\'live\'',
    }
    datasets = {}
    for name, query in queries.items():
        dataset = db.session.query(SqlaTable).filter_by(table_name='bt_'+name, database_id=database.id).one_or_none()
        if dataset is None:
            dataset = SqlaTable(table_name='bt_'+name, database_id=database.id, schema='mart')
            db.session.add(dataset)
        dataset.sql = query
        dataset.owners = [admin]
        db.session.commit()
        # PostgreSQL cursor metadata remains available even before the first event.
        with database.get_sqla_engine() as engine:
            raw = engine.raw_connection()
            try:
                cursor = raw.cursor()
                cursor.execute('SELECT * FROM (' + query + ') metadata LIMIT 0')
                descriptions = list(cursor.description)
            finally:
                raw.close()
        types = {16:'BOOLEAN',20:'BIGINT',21:'SMALLINT',23:'INTEGER',25:'TEXT',1043:'VARCHAR',
                 700:'REAL',701:'DOUBLE PRECISION',1700:'NUMERIC',1114:'TIMESTAMP',1184:'TIMESTAMP WITH TIME ZONE'}
        old = {c.column_name:c for c in dataset.columns}
        columns = []
        for description in descriptions:
            column = old.get(description.name) or TableColumn(column_name=description.name)
            column.type = types.get(description.type_code,'TEXT')
            column.is_dttm = description.type_code in (1114,1184)
            column.groupby = True
            column.filterable = True
            columns.append(column)
        dataset.columns = columns
        dataset.main_dttm_col = next((c.column_name for c in columns if c.is_dttm),None)
        db.session.commit()
        datasets[name] = dataset

    with database.get_sqla_engine() as engine:
        with engine.connect() as connection:
            latest = connection.execute(text("select run_id from mart.freshness where namespace='live' order by processed_at desc limit 1")).scalar()

    def metric(label, expression):
        return {'expressionType': 'SQL', 'sqlExpression': expression, 'label': label, 'optionName': label}

    def chart(name, source, kind='table', **options):
        dataset = datasets[source]
        form = {'datasource':f'{dataset.id}__table','viz_type':kind,'time_range':'No filter',
                'row_limit':1000,'color_scheme':'battleground','adhoc_filters':[], **options}
        existing = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
        if existing is None:
            existing = Slice(slice_name=name)
            db.session.add(existing)
        existing.datasource_id = dataset.id
        existing.datasource_type = 'table'
        existing.viz_type = kind
        existing.params = json.dumps(form)
        existing.owners = [admin]
        db.session.commit()
        return existing

    def table(name, source, columns, **options):
        return chart(name,source,query_mode='raw',all_columns=columns,include_time=False,
                     order_by_cols=[],page_length=15,table_timestamp_format='%Y-%m-%d %H:%M:%S',**options)

    def number(name,source,expression,**options):
        return chart(name,source,'big_number_total',metric=metric(name,expression),y_axis_format=',.0f',**options)

    def line(name,source,time_column,expression,groupby=None,where=None):
        filters=[] if where is None else [{'expressionType':'SQL','sqlExpression':where,'clause':'WHERE'}]
        return chart(name,source,'echarts_timeseries_line',x_axis=time_column,granularity_sqla=time_column,
                     time_grain_sqla=None,metrics=[metric(name,expression)],groupby=groupby or [],
                     adhoc_filters=filters,show_legend=True,rich_tooltip=True,
                     x_axis_time_format='%H:%M:%S',y_axis_format=',.2f',truncate_metric=True,
                     only_total=True,show_value=False,markerEnabled=True)

    boards = [
      ('live-leaderboard','Live Leaderboard',[
        number('Eliminations','leaderboard','SUM(kills)'),
        number('Damage dealt','leaderboard','SUM(damage)'),
        number('Completed eligible matches','match_overview',"COUNT(*) FILTER (WHERE status IN ('final','corrected'))"),
        number('Winners','leaderboard','SUM(wins)'),
        table('Player standings — project score','leaderboard',['rank','player_id','match_id','kills','wins','score','damage','placement','average_placement','status','processed_at']),
        table('Run freshness — UTC','freshness',['run_id','source_last_event_at','processed_at','pending_late']),
      ]),
      ('match-operations','Match Operations',[
        number('Running matches','match_overview',"COUNT(*) FILTER (WHERE status='running')"),
        number('Reconciled matches','match_overview',"COUNT(*) FILTER (WHERE status IN ('final','corrected'))"),
        line('Alive players','match_timeline','window_start','MIN(alive_players)',['match_id']),
        line('Eliminations per 10 seconds','match_timeline','window_start','SUM(eliminations)',['match_id']),
        table('Manifest completeness','match_overview',['match_id','status','actual_gameplay_count','matched_ids','expected_gameplay_count','processed_at']),
        table('Final placements','leaderboard',['placement','player_id','match_id','kills','damage','status']),
      ]),
      ('server-health','Server Health',[
        table('Current sessions — do not sum across windows','server_sessions',['server_id','region','current_online_players','current_connections'],server_pagination=False),
        line('Synthetic latency p95 (ms)','server_health','window_start','MAX(latency_p95_ms)',['server_id']),
        line('Packet loss (%)','server_health','window_start','100.0*SUM(packets_lost)/NULLIF(SUM(packets_sent),0)',['server_id']),
        line('CPU (%)','server_health','window_start','AVG(cpu_pct)',['server_id']),
        line('Memory (%)','server_health','window_start','AVG(memory_pct)',['server_id']),
        line('Tick duration (ms; target 16.667)','server_health','window_start','AVG(tick_ms)',['server_id']),
        table('Injected incident intervals — synthetic','server_health',['server_id','window_start','incident','latency_p95_ms','packet_loss_pct','status'],adhoc_filters=[{'expressionType':'SQL','sqlExpression':'incident = true','clause':'WHERE'}]),
      ]),
      ('pipeline-reliability','Pipeline Reliability',[
        line('Persisted events per second','pipeline_health','sampled_at','MAX(value)',where="metric='persisted_events_per_second'"),
        line('Checkpoint-committed Kafka lag','pipeline_health','sampled_at','SUM(value)',where="metric='committed_lag'"),
        line('Emitted-to-processed p95 (seconds)','pipeline_health','sampled_at','MAX(value)',where="metric='delay_p95_seconds'"),
        line('Checkpoint duration (ms)','pipeline_health','sampled_at','MAX(value)',where="metric='checkpoint_duration_ms'"),
        table('Current measured counters and health','pipeline_current',['metric','labels','value','sampled_at']),
      ]),
    ]
    css = '''.dashboard-content {background:#11151b;color:#e6eaf0;} .dashboard-component-chart-holder,.dashboard-component-tabs-content {background:#1c232c!important;border-radius:8px;} .dashboard-component-header,.header-title,.chart-header,.dashboard-markdown {color:#e6eaf0!important;} .dashboard-component-chart-holder table,.dashboard-component-chart-holder th,.dashboard-component-chart-holder td {background:#1c232c!important;color:#e6eaf0!important;border-color:#37414e!important;} .big_number_total {color:#e8b45b!important;}'''
    css += '.dashboard-component-chart-holder .header-title,.dashboard-component-chart-holder .slice-header,.dashboard-component-chart-holder [class*=Header] {color:#e6eaf0!important;} .dashboard-component-chart-holder .header-line {color:#e8b45b!important;} .dashboard-component-chart-holder svg text {fill:#c8d3df;}'
    css += '.dashboard-component-chart-holder .header-title a {color:#e6eaf0!important;} .dashboard-component-chart-holder label {color:#c8d3df!important;}'
    for slug, title, charts in boards:
        dashboard = db.session.query(Dashboard).filter_by(slug=slug).one_or_none()
        if dashboard is None:
            dashboard = Dashboard(slug=slug)
            db.session.add(dashboard)
        dashboard.dashboard_title = title
        dashboard.published = True  # Available to authenticated users; no anonymous public role.
        dashboard.owners = [admin]
        dashboard.slices = charts
        dashboard.css = css
        position = {'DASHBOARD_VERSION_KEY':'v2','ROOT_ID':{'id':'ROOT_ID','type':'ROOT','children':['GRID_ID']},
                    'GRID_ID':{'id':'GRID_ID','type':'GRID','children':[],'parents':['ROOT_ID']}}
        banner='HEADER-'+slug
        position[banner]={'id':banner,'type':'MARKDOWN','children':[],'parents':['ROOT_ID','GRID_ID'],
            'meta':{'code':f'### {title}\nSynthetic gameplay and server telemetry · UTC · Project scoring, no official affiliation.\n'+('Measured installation-wide pipeline data. Missing metrics are unavailable, not zero.' if slug=='pipeline-reliability' else 'Select exactly one run. Final means reconciled; provisional means incomplete. Read timestamps before interpreting freshness.'),'height':14,'width':12}}
        position['GRID_ID']['children'].append(banner)
        groups = [charts[:4],charts[4:5],charts[5:]] if slug=='live-leaderboard' else [charts[i:i+2] for i in range(0,len(charts),2)]
        for i,group in enumerate(groups):
            row_id=f'ROW-{slug}-{i}'; children=[]
            position['GRID_ID']['children'].append(row_id)
            position[row_id]={'id':row_id,'type':'ROW','children':children,'parents':['ROOT_ID','GRID_ID'],'meta':{'background':'BACKGROUND_TRANSPARENT'}}
            for c in group:
                key=f'CHART-{c.id}';children.append(key)
                position[key]={'id':key,'type':'CHART','children':[],'parents':['ROOT_ID','GRID_ID',row_id],
                    'meta':{'chartId':c.id,'sliceName':c.slice_name,'width':12//len(group),'height':(22 if slug=='live-leaderboard' and i==0 else 42)}}
        filters=[]
        if slug!='pipeline-reliability':
            first=datasets['server_health' if slug=='server-health' else 'leaderboard']
            fields=['run_id','server_id','region'] if slug=='server-health' else (['run_id','match_id','region','map'] if slug=='live-leaderboard' else ['run_id','match_id'])
            for column in fields:
                mask={}
                if column=='run_id' and latest:
                    mask={'extraFormData':{'filters':[{'col':column,'op':'IN','val':[latest]}]},'filterState':{'value':[latest],'label':latest}}
                filters.append({'id':f'NATIVE_FILTER-{slug}-{column}','name':column.replace('_',' ').title(),
                    'filterType':'filter_select','targets':[{'datasetId':first.id,'column':{'name':column}}],
                    'defaultDataMask':mask,'cascadeParentIds':[],
                    'controlValues':{'multiSelect':column!='run_id','enableEmptyFilter':column=='run_id','defaultToFirstItem':column=='run_id','searchAllOptions':True},
                    'scope':{'rootPath':['ROOT_ID'],'excluded':[]},'type':'NATIVE_FILTER'})
        dashboard.position_json=json.dumps(position)
        dashboard.json_metadata=json.dumps({'refresh_frequency':15,'timed_refresh_immune_slices':[],
            'color_scheme':'battleground','native_filter_configuration':filters,'expanded_slices':{}})
        db.session.commit()
        print(f'Authored dashboard: /superset/dashboard/{slug}/ ({len(charts)} charts)')
