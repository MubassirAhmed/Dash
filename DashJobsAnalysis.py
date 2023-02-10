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
    os.environ['SNOW_USER']='mvbasxhr'
    os.environ['SNOW_PWD']='ReLife!23'
    os.environ['SNOW_ACCOUNT']='kl20451.ca-central-1.aws'
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


con = get_snowflake_connector()
cur =  con.cursor()
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server






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
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,'2023-01-06 20:12:02'::timestamp)
    AND REMOVE_TITLES = 0;
'''
cur.execute(query4)
table4 = cur.fetch_pandas_all()
for i in range(table4.shape[0]):
    table4.TITLE[i]  = " ".join(["[",table4.TITLE[i],"](",table4.JOB_LINK[i],")"])
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
      Input('table4-time-range-type','value')
      ])

def update_table4(time_range_slider_value, time_range_type):
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


# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([
            
            dbc.Row([
                dbc.Col(fourthCard, width=True),
                ],),

            ],
            )

# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True)

