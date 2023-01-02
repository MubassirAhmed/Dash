"""  
#? Sort by:
dcc.Dropdown(
id ='keyword_filter',
options = {
    'APPSPERHOUR':'Fastest Growing',
    'NOAPPLICANTS':'No. of Applicants',
    'POSTEDTIMEAGO':'YoE'
},
value='none'
),"""


"""
#? Filter by keywords:
dcc.Dropdown(
id ='keyword_filter',
options = {
    'noApplicants':'No.Of Apps',
    'TimePosted':'TimePosted',
    'YoE':'YoE'
},
value='none'
),


#? Time Posted:
dcc.Dropdown(
id ='keyword_filter',
options = {
    'noApplicants':'No.Of Apps',
    'TimePosted':'TimePosted',
    'YoE':'YoE'
},
value='none'
),"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table  
import os
import snowflake.connector
import dash_bootstrap_components as dbc





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

app = Dash(external_stylesheets=[dbc.themes.SKETCHY])
application = app.server





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
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,current_timestamp);
'''
cur.execute(query1)
table1 = cur.fetch_pandas_all()
for i in range(table1.shape[0]):
    table1.TITLE[i]  = "["+table1.TITLE[i]+"]("+table1.JOB_LINK[i]+")"
table1['NOAPPLICANTS'] = table1['NOAPPLICANTS'].astype(int)
table1 = table1.drop(columns=["JOB_LINK"])  

table1_layout= dash_table.DataTable(
    style_cell={'whiteSpace': 'normal',
                'height': 'auto',
                'textAlign': 'center',
                 'lineHeight': '15px'},
    data=table1.to_dict('records'),
    columns=[{'id': x, 'name': x, 'presentation': 'markdown'} if x == 'TITLE' else {'id': x, 'name': x} for x in table1.columns],
    style_as_list_view=True,
    style_table={'height': '475px', 'overflowY': 'auto'},
    sort_action='native',
    )

firstCard  = dbc.Card(dbc.CardBody(table1_layout))



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
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,current_timestamp);
'''
cur.execute(query2)
data2 = cur.fetch_pandas_all().apply(pd.to_numeric)
data2.columns = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']
data2 = data2.sum()
chart2 = px.bar(data2, x=data2.index, y=data2,
                labels={
                     "data2.index": "Years of Experience Required",
                     "data2": "No. of Jobs",
                 },
                #template="simple_white"
                )
chart2_layout= dcc.Graph(id='YoE',figure=chart2)
secondCard  = dbc.Card(dbc.CardBody([
                        html.H6("Demand for Years of Experience", style={'text-align': 'center'}),
                        html.Div(
                            dcc.RadioItems([
                            {
                             'label':html.Div(['Hours ago'],style={'padding':'0px 20px 10px', 'margin-top':'-5px'}),
                             'value':'hour',
                            },
                            {
                             'label':html.Div(['Days ago'],style={'padding':'0px 20px 10px', 'margin-top':'-5px'}),
                             'value':'day',
                            },
                            {
                             'label':html.Div(['Weeks ago'],style={'padding':'0px 20px 10px', 'margin-top':'-5px'}),
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
                        html.Div(id='YoE-Print'),

                            ]))
@app.callback(
    Output('my-output', 'children'),
    Input('chart2-time-range-slider', 'value')
)
def update_output_div(chart2_time_range_value):
    return f'Output: {chart2_time_range_value}'



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
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,current_timestamp);
'''
cur.execute(query3)
data3 = cur.fetch_pandas_all().apply(pd.to_numeric).sum()
chart3 = px.bar(data3, x=data3.index, y=data3,
        #template="simple_white"
        )
chart3_layout = dcc.Graph(id='skills',figure=chart3)
thirdCard = dbc.Card(dbc.CardBody([
            dcc.RadioItems(
            ['Hours', 'Days'],
            'Hours',
            id='chart3-time-range-type',
            labelStyle={'display': 'inline-block', 'marginTop': '5px'}
            ),
            html.H6("Skills/Tools Required", style={'text-align': 'center'}),
            chart3_layout,
            html.Div(dcc.Slider(min=1,max=24,step=None,
                #id='chart2-time-range-slider',
                value=5,
                marks={
                1:'1 hr',
                2:'2 hrs ago',
                3:'3 hrs ago',
                5:'5 hrs ago',
                10:'10 hrs ago',
                24:'24 hrs ago'
                }
                ), style={ 'padding': '0px 20px 20px 20px',  #'width': '49%',
                        })
            ]))



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
    JOB_LINK
FROM unique_postings
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,current_timestamp);
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
                                style_cell={'whiteSpace': 'normal','height': 'auto','textAlign': 'center',},
                                data=table4.to_dict('records'),
                                columns=[{'id': x, 'name': x, 'presentation': 'markdown'} if x == 'Title' else {'id': x, 'name': x} for x in table4.columns],
                                style_as_list_view=True,
                                style_table={'height': '300px', 'overflowY': 'auto'},
                                    sort_action='native',
                                filter_action='native',


                                )
fourthCard = table4_layout   

#TODO 4. Jobs Posted | No. of apps vs Time (24 Hours), T-s Chart [radio buttons: Time of Day, Week, Month,  JobsPosted and NoOfApps as two colors with legends]


#TODO 5. No experiece | No experience & entry level postings vs Time, T-s, over the month

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

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

            dbc.Row([
                dbc.Col(html.Div(firstCard), width=True),
                dbc.Col([html.Div(secondCard),], width=5),
                dbc.Col([html.Div(thirdCard),], width=True),
                ],),
            
            html.Br(),
            
            dbc.Row(
                dbc.Col(fourthCard, width=True)), 
            ])

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components




@app.callback(
     [Output('YoE', 'figure'),Output('YoE-Print', 'children')],
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
        WHERE SNOW_COL_TIMESTAMP >= dateadd({0},{1},current_timestamp);
        '''.format(time_range_type,dateAdd_Step)
    cur.execute(query2)
    data2 = cur.fetch_pandas_all().apply(pd.to_numeric)
    data2.columns = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']
    data2 = data2.sum()
    chart2 = px.bar(data2, x=data2.index, y=data2,
                    labels={
                         "data2.index": "Years of Experience Required",
                         "data2": "No. of Jobs",
                     },
                    #template="simple_white"
                    )
    
    return chart2, query2

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
    app.run_server(debug=True)