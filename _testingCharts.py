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
application = app.server





#TODO 1. New Postings in the last hour, table [col: title(with link), year(filter & sort), keywords(filter)] 


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
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,current_timestamp);
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
WHERE SNOW_COL_TIMESTAMP >= dateadd(hour,-2,current_timestamp);
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
                                css=[{
                                    'selector': '.dash-spreadsheet td div',
                                    'rule': '''
                                        line-height: 15px;
                                        max-height: 30px; min-height: 30px; height: 30px;
                                        display: block;
                                        overflow-y: hidden;
                                    '''
                                }],
                                # css=[{
                                # 'selector': '.dash-spreadsheet tr .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr',
                                # 'rule': 'height: 10px; min-height: 10px;'
                                # }],

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
    WHERE SNOW_COL_TIMESTAMP >= dateadd({},{},current_timestamp)
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

            

            dbc.Row([
                dbc.Col([html.Div(secondCard),], width=True),
                dbc.Col(fourthCard, width=True),
                ],),
            
            ],
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
        WHERE SNOW_COL_TIMESTAMP >= dateadd({0},{1},current_timestamp);
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
    app.run_server(debug=True)