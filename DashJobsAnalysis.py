import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table  
import os
import snowflake.connector
import dash_bootstrap_components as dbc
from num2words import num2words


white_button_style = {'background-color': 'white',
                      'color': 'black',
                      'height': '50px',
                      'width': '100px',
                      'margin-top': '50px',
                      'margin-left': '50px'}

def get_snowflake_connector():
    os.environ['SNOW_USER']='mvbashxr'
    os.environ['SNOW_PWD']='ReLife!23'
    os.environ['SNOW_ACCOUNT']='ep66367.ca-central-1.aws'
    os.environ['SNOW_WH']='AIRFLOW_ELT_WH'
    os.environ['SNOW_DB']='AIRFLOW_ELT_DB'
    os.environ['SNOW_SCH']='AIRFLOW_ELT_SCHEMA'

    con = snowflake.connector.connect(
    user=os.getenv('SNOW_USER'),
    password=os.getenv('SNOW_PWD'),
    account=os.getenv('SNOW_ACCOUNT'),
    warehouse=os.getenv('SNOW_WH'),
    database=os.getenv('SNOW_DB'),
    schema=os.getenv('SNOW_DH')
    )
    # IDK y but snowflake makes me choose a schema before i can do anything
    con.cursor().execute("USE SCHEMA AIRFLOW_ELT_SCHEMA")
    return con



def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])
con = get_snowflake_connector()
cur =  con.cursor()
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server





#TODO 1. New Postings in the last hour, table [col: title(with link), year(filter & sort), keywords(filter)] 
query1= '''
with unique_postings as (
    SELECT *
        from job_postings
        qualify row_number() over(partition by job_id order by one) = 1
    )
    select
    TITLE,
    NOAPPLICANTS,
    JOB_LINK,
    COMPANY

FROM unique_postings
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,'2023-01-06 20:12:02'::timestamp
)
Order by SNOW_COL_TIMESTAMP desc;
'''
cur.execute(query1)
table1 = cur.fetch_pandas_all()
for i in range(table1.shape[0]):
    table1.TITLE[i]  = "["+table1.TITLE[i]+"]("+table1.JOB_LINK[i]+")"
table1['NOAPPLICANTS'] = table1['NOAPPLICANTS'].astype(int)
table1 = table1.drop(columns=["JOB_LINK"])  

table1_layout=dash_table.DataTable(
    id='table1',
    data=table1.to_dict('records'),
    columns=[{'id': x, 'name': x, 'presentation': 'markdown'} if x == 'TITLE' else {'id': x, 'name': x} for x in table1.columns],
    style_cell={
        'whiteSpace': 'normal',
        'height': 'auto',
        'textAlign': 'center',
        'lineHeight': '15px',
        'background-color':'#252B34', 'color':'#6DFFA0', 'border-color':'red'
        },
    style_as_list_view = True,
    style_table={'height': '475px', 'overflowY': 'auto'},
    sort_action='native',
    )

firstCard  = dbc.Card(dbc.CardBody([
                html.H6("New Jobs in last 5 hrs: ", style={'text-align': 'center'}),
                html.P("Quick filter by experience using the bars in the Demand for Years of Experience chart"),
                html.Div(id='YoE-Print', style={'text-align': 'center', 'color': '#8DC6FF', 'font-family':'Times New Roman'}),
                table1_layout]))


def HDW_radio_button(chartName):
    return html.Div(
        dcc.RadioItems([
        {
         'label':html.Div(['Hours ago'],style={'padding':'0px 20px 10px', 'margin-top':'-15px', 'color':'#8DC6FF'}),
         'value':'hour',
        },
        {
         'label':html.Div(['Days ago'],style={'padding':'0px 20px 10px', 'margin-top':'-15px', 'color':'#8DC6FF'}),
         'value':'day',
        },
        {
         'label':html.Div(['Weeks ago'],style={'padding':'0px 20px 10px', 'margin-top':'-15px', 'color':'#8DC6FF'}),
         'value':'week',
        }

        ], value='hour',
        id='{}-time-range-type'.format(chartName),
        inline=True)
    , style={'padding': '0px 20px 20px 0px', 'color':'#8DC6FF'})

#TODO 2. Years of Exp Required
    #? clicking on smth here will send it as an input to The Table
query2= '''
with unique_postings as (
    SELECT *
        from job_postings
        qualify row_number() over(partition by job_id order by one) = 1
    )
SELECT ZERO, ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE, THIRTEEN, FOURTEEN, FIFTEEN
FROM unique_postings
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,'2023-01-06 20:12:02'::timestamp);
'''
cur.execute(query2)
data2 = cur.fetch_pandas_all().apply(pd.to_numeric)
data2.columns = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']
data2 = data2.sum()
chart2 = px.bar(data2, x=data2.index, y=data2,template='simple_white'
                ).update_layout(
    xaxis_title="Years of Experience Required", yaxis_title="No. of Jobs", font_color="#6DFFA0",
    font_family="Times New Roman",paper_bgcolor='#252B34',
    plot_bgcolor='#252B34',).update_traces(marker_color='#6DFFA0')
chart2_layout= dcc.Graph(id='YoE',figure=chart2)
secondCard  = dbc.Card(dbc.CardBody([


                        html.H6("Demand for Years of Experience", style={'text-align': 'center'}),


                        html.Div(
                            dcc.RadioItems([
                            {
                             'label':html.Div(['Hours ago'],style={'padding':'0px 20px 10px', 'margin-top':'-20px', 'color':'#8DC6FF'}),
                             'value':'hour',
                            },
                            {
                             'label':html.Div(['Days ago'],style={'padding':'0px 20px 10px', 'margin-top':'-20px', 'color':'#8DC6FF'}),
                             'value':'day',
                            },
                            {
                             'label':html.Div(['Weeks ago'],style={'padding':'0px 20px 10px', 'margin-top':'-20px', 'color':'#8DC6FF'}),
                             'value':'week',
                            }
                            ], value='hour',
                            id='chart2-time-range-type',
                            inline=True)
                        , style={'padding': '0px 20px 20px 0px'}),
                        

                        html.Div([chart2_layout,],style={
                            'margin-top':'-30px',
                            'padding-top':'0px'}),


                        html.Div(dcc.Slider(min=1,max=24,step=None,
                            marks = {i:'{}'.format(i) for i in range(1,25)},
                            value=5,
                            id='chart2-time-range-slider',
                            ), style={ 'padding': '0px 20px 20px 20px',  #'width': '49%',
                                    }),

                        html.Div(id='my-output'),

                            ]))


@app.callback(
    [Output('table1','data'),
    Output('YoE-Print','children')],
    Input('YoE','clickData')
    )

def update_table1_from_chart2(clickData):
    query1= '''
    with unique_postings as (
        SELECT *
            from job_postings
            WHERE {} = 1
            qualify row_number() over(partition by job_id order by one) = 1
        )
        select
        TITLE,
        NOAPPLICANTS,
        JOB_LINK,
        COMPANY
    FROM unique_postings
    WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,'2023-01-06 20:12:02'::timestamp)
    Order by SNOW_COL_TIMESTAMP desc;
    '''.format( num2words(clickData['points'][0]['x']) )
    cur.execute(query1)
    table1 = cur.fetch_pandas_all()
    for i in range(table1.shape[0]):
        table1.TITLE[i]  = "["+table1.TITLE[i]+"]("+table1.JOB_LINK[i]+")"
    table1['NOAPPLICANTS'] = table1['NOAPPLICANTS'].astype(int)
    table1 = table1.drop(columns=["JOB_LINK"]) 

    YoEfilterLabel = 'Jobs requiring {} years of experience'.format(clickData['points'][0]['x'])
        

    return table1.to_dict('records'),YoEfilterLabel

#TODO 3. SQL/ keywords histogram histogram, buttons/slider for time-line 
    #? clicking on smth here will send it as an input to The Table
#* Chart 3
"""'sql','python','airflow','etl','snowflake','aws','azure','gcp','bigquery','spark',
                        'hadoop','hive','lambda','dbt', 'google','amazon','microsoft','bi','tableau',
                   'power','looker', 'excel','javascript','react','vue','redshift','databricks','powerbi','iac','terraform','ansible','kubernete','k8s','ci/cd','ci','dataops','kafka'"""
query3= '''
with unique_postings as (
    SELECT *
        from job_postings
        qualify row_number() over(partition by job_id order by one) = 1
    )
SELECT 
    SQL,
    PYTHON,
    AIRFLOW,SNOWFLAKE,BIGQUERY,DBT,GCP
FROM unique_postings
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,'2023-01-06 20:12:02'::timestamp);
'''
cur.execute(query3)
data3 = cur.fetch_pandas_all().apply(pd.to_numeric).sum()
chart3 = px.bar(data3, x=data3.index, y=data3,template='simple_white' 
        ).update_layout(
    xaxis_title="Skills Required", yaxis_title="No. of Jobs", font_color="#6DFFA0",
    font_family="Times New Roman",paper_bgcolor='#252B34',
    plot_bgcolor='#252B34',).update_traces(marker_color='#6DFFA0')
chart3_layout = dcc.Graph(id='skills',figure=chart3)
thirdCard = dbc.Card(dbc.CardBody([
            html.H6("Skills/Tools Required", style={'text-align': 'center'}),

            HDW_radio_button('chart3'),

            html.Div(chart3_layout, style={
                            'margin-top':'-30px',
                            'padding-top':'0px'}),

            html.Div(dcc.Slider(min=1,max=24,step=None,
                            marks = {i:'{}'.format(i) for i in range(1,25)},
                            value=5,
                            id='chart3-time-range-slider',
                            ), style={ 'padding': '0px 20px 20px 20px',  #'width': '49%',
                                    }),
            ]))

# @app.callback(
#     [Output('table1','data'),
#     Output('skills-Print','children')],
#     Input('skills','clickData')
#     )

# def update_table1_from_chart3(clickData):
#     query3= '''
#     with unique_postings as (
#         SELECT *
#             from job_postings
#             WHERE {} = 1
#             qualify row_number() over(partition by job_id order by one) = 1
#         )
#         select
#         TITLE,
#         NOAPPLICANTS,
#         JOB_LINK,
#         COMPANY
#     FROM unique_postings
#     WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,


)
#     Order by SNOW_COL_TIMESTAMP desc;
#     '''.format( num2words(clickData['points'][0]['x']) )
#     cur.execute(query3)
#     table1 = cur.fetch_pandas_all()
#     for i in range(table1.shape[0]):
#         table1.TITLE[i]  = "["+table1.TITLE[i]+"]("+table1.JOB_LINK[i]+")"
#     table1['NOAPPLICANTS'] = table1['NOAPPLICANTS'].astype(int)
#     table1 = table1.drop(columns=["JOB_LINK"]) 

#     skillsfilterLabel = 'Jobs requiring {}'.format(clickData['points'][0]['x'])
        

#     return table1.to_dict('records'),skillsfilterLabel


@app.callback(
     Output('skills', 'figure'),
     [Input('chart3-time-range-slider','value'),
      Input('chart3-time-range-type','value'),
      ])

def update_chart3(time_range_slider_value, time_range_type):
    if time_range_type == 'hour':
        dateAdd_Step = -time_range_slider_value + 3
    else:
        dateAdd_Step = -time_range_slider_value
    query3= '''
    with unique_postings as (
        SELECT *
            from job_postings
            qualify row_number() over(partition by job_id order by one) = 1
        )
        select
        SQL,
        PYTHON,
        AIRFLOW,SNOWFLAKE,BIGQUERY,DBT,GCP
        FROM unique_postings
        WHERE SNOW_COL_TIMESTAMP >= dateadd({0},{1},'2023-01-06 20:12:02'::timestamp);
        '''.format(time_range_type,dateAdd_Step)
    cur.execute(query3)
    data3 = cur.fetch_pandas_all().apply(pd.to_numeric).sum()
    chart3 = px.bar(data3, x=data3.index, y=data3,template = 'simple_white' 
    ).update_layout(
    xaxis_title="Skills Required", yaxis_title="No. of Jobs", font_color="#6DFFA0",
    font_family="Times New Roman",paper_bgcolor='#252B34',
    plot_bgcolor='#252B34',).update_traces(marker_color='#6DFFA0')
    return chart3

@app.callback(
    [Output('chart3-time-range-slider','marks'),
    Output('chart3-time-range-slider','max')],
    Input('chart3-time-range-type','value'),)

def update_chart3_slider(chart3_time_range_type):
    if chart3_time_range_type == 'hour':
        marks={i:'{}'.format(i) for i in range(1,25)}
        max = 24
    else: 
        if chart3_time_range_type == 'day':
            marks={i:'{}'.format(i) for i in range(1,8)}
            max = 7

        else:
            if chart3_time_range_type == 'week':
                marks={i:'{}'.format(i) for i in range(1,5)}
                max = 4

    return marks,max




#TODO 4. Table for Browsing Jobs + Lambda scraper
query4= '''
with unique_postings as (
    SELECT *
        from job_postings
        qualify row_number() over(partition by job_id order by one) = 1
    )
SELECT 
    TITLE,
    NOAPPLICANTS,
    COMPANY,
    JOB_LINK,
    description
FROM unique_postings
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,'2023-01-06 20:12:02'::timestamp);
'''
cur.execute(query4)
table4 = cur.fetch_pandas_all()
for i in range(table4.shape[0]):
    table4.TITLE[i]  = " ".join(["[",table4.TITLE[i],"](",table4.JOB_LINK[i],")"])
#table4 = table4.loc[:, ['TITLE', 'NOAPPLICANTS', 'COMPANY',]]
table4=table4.drop(columns=['JOB_LINK'])
table4['NOAPPLICANTS'] = table4['NOAPPLICANTS'].astype(int)
table4 = table4.rename(columns={'TITLE': 'Title', 'NOAPPLICANTS': 'No. of Applications', 'COMPANY':'Company'})
table4_layout = dash_table.DataTable(
                                id='filteringTable',
                                style_cell={'whiteSpace': 'normal','height': 'auto',
                                            'minWidth': '180px', 'width': '180px', 'maxWidth': '180px',
                                            'minHeight':'50px','height':'50px','maxHeight':'50px',
                                            'textAlign': 'left','lineHeight': '15px',
                                            'overflow': 'hidden',
                                            'textOverflow': 'ellipsis',
                                            'maxWidth': 0, 'background-color':'#252B34', 'color':'#6DFFA0',},
                                data=table4.to_dict('records'),
                                columns=[{'id': x, 'name': x, 'presentation': 'markdown'} if x == 'Title' else {'id': x, 'name': x} for x in table4.columns],
                                style_as_list_view=True,
                                style_table={'height': '600px', 'overflowY': 'auto', 'overflowX': 'auto'},
                                sort_action='native',
                                filter_action='native',
                                # tooltip_data=[
                                #     {
                                #         column: {'value': str(value), 'type': 'markdown'}
                                #         for column, value in row.items()
                                #     } for row in table4.to_dict('records')
                                # ],
                                # tooltip_duration=None,
                                css=[{
                                    'selector': '.dash-spreadsheet td div',
                                    'rule': '''
                                        line-height: 15px;
                                        max-height: 30px; min-height: 30px; height: 30px;
                                        display: block;
                                        overflow-y: hidden;
                                    '''
                                }],

                                )
fourthCard = dbc.Card(dbc.CardBody([
                html.H6("Table for filtering jobs by keywords", style={'text-align': 'center'}),

                table4_layout,

                HDW_radio_button('table4'),

                html.Div(dcc.Slider(min=1,max=24,step=None,
                            marks = {i:'{}'.format(i) for i in range(1,25)},
                            value=5,
                            id='table4-time-range-slider',
                            ), style={ 'padding': '0px 20px 20px 20px',  #'width': '49%',
                                    }),

                
                ])   )

@app.callback(
     Output('filteringTable', 'data'),
     [Input('table4-time-range-slider','value'),
      Input('table4-time-range-type','value'),
      Input('YoE','clickData')
      ])

def update_table4(time_range_slider_value, time_range_type,clickData):
    if time_range_type == 'hour':
        dateAdd_Step = -time_range_slider_value + 3
    else:
        dateAdd_Step = -time_range_slider_value


    query4_part1= '''
    with unique_postings as (
    SELECT *
        from job_postings

    '''

    query4_part2 = '''
        qualify row_number() over(partition by job_id order by one) = 1
    )
    SELECT 
        TITLE,
        NOAPPLICANTS,
        COMPANY,
        JOB_LINK,
        description
    FROM unique_postings
    WHERE SNOW_COL_TIMESTAMP >= dateadd({},{},'2023-01-06 20:12:02'::timestamp)
    Order by SNOW_COL_TIMESTAMP desc;;
    '''


    if clickData['points'][0]['x'] is None:
        query4 = query4_part1 + \
                 query4_part2.format(time_range_type, dateAdd_Step)
    else:
        query4 = query4_part1 + \
                 'WHERE {} = 1'.format(num2words(clickData['points'][0]['x'])) +  \
                 query4_part2.format(time_range_type, dateAdd_Step)


    cur.execute(query4)
    table4 = cur.fetch_pandas_all()
    for i in range(table4.shape[0]):
        table4.TITLE[i]  = " ".join(["[",table4.TITLE[i],"](",table4.JOB_LINK[i],")"])
    table4=table4.drop(columns=['JOB_LINK'])
    table4['NOAPPLICANTS'] = table4['NOAPPLICANTS'].astype(int)
    table4 = table4.rename(columns={'TITLE': 'Title', 'NOAPPLICANTS': 'No. of Applications', 'COMPANY':'Company'})
    return table4.to_dict('records')

@app.callback(
    [Output('table4-time-range-slider','marks'),
    Output('table4-time-range-slider','max')],
    Input('table4-time-range-type','value'),)

def update_table4_slider(table4_time_range_type):
    if table4_time_range_type == 'hour':
        marks={i:'{}'.format(i) for i in range(1,25)}
        max = 24
    else: 
        if table4_time_range_type == 'day':
            marks={i:'{}'.format(i) for i in range(1,8)}
            max = 7

        else:
            if table4_time_range_type == 'week':
                marks={i:'{}'.format(i) for i in range(1,5)}
                max = 4

    return marks,max



#TODO 5. Jobs Posted | No. of apps vs Time (24 Hours), T-s Chart [radio buttons: Time of Day, Week, Month,  JobsPosted and NoOfApps as two colors with legends]

query5="""
with unique_postings as (
    SELECT *
        from job_postings
        qualify row_number() over(partition by job_id order by one) = 1
    )
select
    count(job_id) numberOfJobs,
    hour --dayofweek,nameofmonth,dayofmonth
from unique_postings
where hour is not null
AND SNOW_COL_TIMESTAMP >= dateadd(week,-1,'2023-01-06 20:12:02'::timestamp)
group by hour
order by hour desc;

"""
cur.execute(query5)
data5 = cur.fetch_pandas_all().apply(pd.to_numeric) 
chart5 = px.line(data5,x=data5['HOUR'],y=data5['NUMBEROFJOBS'], template='none').update_layout(
    xaxis_title="Time of Day", yaxis_title="No. of Jobs Posted", font_color="#6DFFA0",
    font_family="Times New Roman",paper_bgcolor='#252B34',
    plot_bgcolor='#252B34',).update_traces(marker_color='#6DFFA0').update_traces(line_color='#6DFFA0', line_width=5)
chart5_layout = dcc.Graph(id='JvsT',figure=chart5)
fifthCard = dbc.Card(dbc.CardBody([
                        html.H6("No. of Jobs posted v.s. Time", style={'text-align': 'center'}),

                        html.Div(
                        dcc.RadioItems([
                        {
                         'label':html.Div(['Time of Day'],style={'padding':'0px 20px 10px', 'margin-top':'-5px', 'color':'#8DC6FF'}),
                         'value':'hour',
                        },
                        {
                         'label':html.Div(['Day of the Week'],style={'padding':'0px 20px 10px', 'margin-top':'-5px', 'color':'#8DC6FF'}),
                         'value':'dayofweek',
                        },

                        ], value='hour',
                        id='chart5-timeline-type',
                        inline=True)
                        , style={'padding': '0px 20px 20px 0px'}),

                        html.Div(chart5_layout, style={
                            'margin-top':'-30px',
                            'padding-top':'0px'}),

                    ]))

@app.callback(
    Output('JvsT','figure'),
    Input('chart5-timeline-type','value'))

def update_chart5(timeline_type):
    query5= f"""
    with unique_postings as (
        SELECT *
            from job_postings
            qualify row_number() over(partition by job_id order by one) = 1
        )
    select
        count(job_id) numberOfJobs,
        {timeline_type} --dayofweek,nameofmonth,dayofmonth
    from unique_postings
    where {timeline_type} is not null
    AND SNOW_COL_TIMESTAMP >= dateadd(week,-1,'2023-01-06 20:12:02'::timestamp)
    group by {timeline_type}
    order by {timeline_type} desc;

    """
    cur.execute(query5)
    data5 = cur.fetch_pandas_all() 
    chart5 = px.line(data5,x=data5[timeline_type.upper()],y=data5['NUMBEROFJOBS'],template='none').update_layout(
    xaxis_title="Time of Day", yaxis_title="No. of Jobs Posted", font_color="#6DFFA0",
    font_family="Times New Roman",paper_bgcolor='#252B34',
    plot_bgcolor='#252B34',).update_traces(line_color='#6DFFA0', line_width=5)
    return chart5



#TODO 6. No experiece | No experience & entry level postings vs Time, T-s, over the month

#TODO 7. Scatter Plot: Software Eng. sector as color, no. of applicants as size, keywords as marks, number of jobs on y, something on x, [Time slider for subsetting]

#TODO 8. Fastest growing jobs in the last 72 hours, T-s; You need proper timestamp to implement this

#TODO 9. Small table with of Companies that are on hiring session.





#* Table 5
#TODO Add a lambda here to do a search


    
#'Link(s)': ['[Google](https://www.google.com)', '[Twitter](https://www.twitter.com), [Facebook](https://www.facebook.com)'],
"""
#* Chart 6 -- here you need to transform the date 
data6 = df.iloc[:,]
fig = px.line(df, x="lifeExp", y="gdpPercap", color="country", text="year")"""

zerothDiv = html.Div([
    ])

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([
            dbc.Row([
                dbc.Col([html.Div([
                    html.H1("Linkedin Job Tracker", style={'text-align': 'left', 'color':'#8DC6FF', 'font-family':'Times New Roman'}),
                    html.H3("This app is built on top of an ELT pipeline, with Python, Dash, SQL, Pandas and CSS. It queries data from the Snowflake warehouse.", style={'text-align': 'left', 'color':'#8DC6FF', 'font-family':'Times New Roman'}),],
                     )],width=8),

                dbc.Col(html.Div([html.A(
                    href="https://github.com/MubassirAhmed/Dash",
                    children=[
                        html.Img(
                            alt="Link to my twitter",
                            src="assets/GitHub_Logo.png",
                            style={'float':'right', 'padding-right':'30px'}
                        ),
                        ]
                    ),
                    html.H6("Checkout the github repo :)")
                    ],style={'display':'inline-block', 'float':'right', 'padding-right':'10px'}), width=4)
            ]),

            
            
            #html.Img(src='assets/GitHub_Logo.png',style={'text-align':'right'}),

            dbc.Row([
                dbc.Col(html.Div(firstCard), width=True),
                dbc.Col([html.Div(secondCard),], width=5),
                dbc.Col([html.Div(thirdCard),], width=True),
                ],),
            
            html.Br(),
            
            dbc.Row([
                dbc.Col(fourthCard, width=True),
                dbc.Col(fifthCard,width=True),
                ],),

            ],
            # style={
            # 'backgroundColor':'blue'
            # } 
            )

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components




@app.callback(
     Output('YoE', 'figure'),
     [Input('chart2-time-range-slider','value'),
      Input('chart2-time-range-type','value'),
      ])

def update_chart2(time_range_slider_value, time_range_type):
    if time_range_type == 'hour':
        dateAdd_Step = -time_range_slider_value + 3
    else:
        dateAdd_Step = -time_range_slider_value
    query2= '''
    with unique_postings as (
        SELECT *
            from job_postings
            qualify row_number() over(partition by job_id order by one) = 1
        )
        SELECT ZERO, ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE, THIRTEEN, FOURTEEN, FIFTEEN
        FROM unique_postings
        WHERE SNOW_COL_TIMESTAMP >= dateadd({0},{1},'2023-01-06 20:12:02'::timestamp);
        '''.format(time_range_type,dateAdd_Step)
    cur.execute(query2)
    data2 = cur.fetch_pandas_all().apply(pd.to_numeric)
    data2.columns = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']
    data2 = data2.sum()
    chart2 = px.bar(data2, x=data2.index, y=data2, template='simple_white'
                    ).update_layout(
    xaxis_title="Years of Experience Required", yaxis_title="No. of Jobs",font_color="#6DFFA0",
    font_family="Times New Roman",paper_bgcolor='#252B34',
    plot_bgcolor='#252B34',).update_traces(marker_color='#6DFFA0')
    
    return chart2

@app.callback(
    [Output('chart2-time-range-slider','marks'),
    Output('chart2-time-range-slider','max')],
    Input('chart2-time-range-type','value'),)

def update_chart2_slider(chart2_time_range_type):
    if chart2_time_range_type == 'hour':
        marks={i:'{}'.format(i) for i in range(1,25)}
        max = 24
    else: 
        if chart2_time_range_type == 'day':
            marks={i:'{}'.format(i) for i in range(1,8)}
            max = 7

        else:
            if chart2_time_range_type == 'week':
                marks={i:'{}'.format(i) for i in range(1,5)}
                max = 4

    return marks,max

# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=False)
