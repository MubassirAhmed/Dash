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
import plotly.express as px  # (version 4.7.0 or higher)
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table  
import os
import snowflake.connector
import dash_bootstrap_components as dbc

external_stylesheets = [dbc.themes.BOOTSTRAP]

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
query= '''
With unique_jobs as (
Select * from job_postings
Qualify row_number() over (partition by job_id order by one) =1
)
Select 
    *
From unique_jobs;
'''
cur.execute(query)
df = cur.fetch_pandas_all()

app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
application = app.server


#TODO 1. New Postings in the last hour, table [col: title(with link), year(filter & sort), keywords(filter)] 
table1 = df
for i in range(table1.shape[0]):
    #[Google](https://www.google.com)
    table1.TITLE[i]  = " ".join(["[",table1.TITLE[i],"](",table1.JOB_LINK[i],")"])
    
# table1['ZERO'] = table1['ZERO'].astype('int64')
# table1 = df.loc[df["ZERO"]==1, "TITLE"].to_dict(dd)



#TODO 2. Jobs Posted | No. of apps vs Time (24 Hours), T-s Chart [radio buttons: Time of Day, Week, Month,  JobsPosted and NoOfApps as two colors with legends]  

#TODO 3. Years of Exp Required
    #? clicking on smth here will send it as an input to The Table

#TODO 4. No experiece | No experience & entry level postings vs Time, T-s, over the month
    
#TODO 5. SQL/ keywords histogram histogram, buttons/slider for time-line 
    #? clicking on smth here will send it as an input to The Table
    
#TODO 6. Table for Browsing Jobs + Lambda scraper

#TODO 7. Scatter Plot: Software Eng. sector as color, no. of applicants as size, keywords as marks, number of jobs on y, something on x, [Time slider for subsetting]

#TODO 8. Fastest growing jobs in the last 72 hours, T-s; You need proper timestamp to implement this

#TODO 9. Small table with of Companies that are on hiring session.




#* Chart 4
data3 = df.loc[:,"SQL":'VUE'].astype('int64').sum()
chart3 = px.bar(data3, x=data3.index, y=data3)

#* Table 5
#TODO Add a lambda here to do a search
table5  = df.loc[:, ['TITLE', 'NOAPPLICANTS', 'COMPANY','JOB_LINK']]
for i in range(table5.shape[0]):
    #[Google](https://www.google.com)
    table5.TITLE[i]  = " ".join(["[",table5.TITLE[i],"](",table5.JOB_LINK[i],")"])
table5 = table5.loc[:, ['TITLE', 'NOAPPLICANTS', 'COMPANY',]]
table5['NOAPPLICANTS'] = table5['NOAPPLICANTS'].astype(int)
table5 = table5.rename(columns={'TITLE': 'Title', 'NOAPPLICANTS': 'No. of Applications', 'COMPANY':'Company'})

    
#'Link(s)': ['[Google](https://www.google.com)', '[Twitter](https://www.twitter.com), [Facebook](https://www.facebook.com)'],
"""
#* Chart 6 -- here you need to transform the date 
data6 = df.iloc[:,]
fig = px.line(df, x="lifeExp", y="gdpPercap", color="country", text="year")"""

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

            dbc.Row([
                dbc.Col([            
                html.Div(
                dbc.Card(
                    dbc.CardBody([
                            html.H4(children='Browse jobs'),
                            dash_table.DataTable(
                                style_cell={'whiteSpace': 'normal','height': 'auto','textAlign': 'center',},
                                data=table5.to_dict('records'),
                                columns=[{'id': x, 'name': x, 'presentation': 'markdown'} if x == 'Title' else {'id': x, 'name': x} for x in table5.columns],
                                #columns=[{'id': c, 'name': c} for c in table5.columns],
                                # fixed_rows={'headers': True},
                                #page_size= min(25, table5.shape[0]), 
                                style_as_list_view=True,
                                style_table={'height': '300px', 'overflowY': 'auto'},
                                style_data={
                                'backgroundColor': 'rgb(50, 50, 50)',
                                'color': 'white'},
                                # page_action="native",
                                # filter_action="native",
                                sort_action="native",
                                # sort_mode="multi",
                                )
                    ])
                )
            ),
            ], width=4),
                dbc.Col([
                    html.Br(),
                    html.Div(
                        dbc.Card(
                            dbc.CardBody([
                                html.H4("Demand for Years of Experience", style={'text-align': 'center'}),
                                dcc.Graph(id='barChart'),
                                dcc.Dropdown(
                                    id ='keyword_filter',
                                    #TODO Dynamically create options from col; DCC here would just be a nice to have feature
                                    options = {
                                        'sql':'SQL',
                                        'python':'Python',
                                        'none':'No filter'
                                    },
                                    value='none'
                                ),
                            ])
                        )
            ), ], width=4),
                dbc.Col([            
                html.Div(
                dbc.Card(
                    dbc.CardBody([    
                            html.Br(),
                            html.Label('Multi-Select Dropdown'),
                            dcc.Dropdown(['New York City', 'Montréal', 'San         Francisco'],
                                            ['Montréal', 'San Francisco'],
                                            multi=True),
                            html.Hr(),
                            dcc.Graph(
                                id='YoE',
                                figure=chart3
                                ),
                            ])
                        )
                    ),], width=4),
            ], align='center'),
            html.Br(),
            
            dbc.Row([
                dbc.Col([ 
                         dash_table.DataTable(
                                style_cell={'whiteSpace': 'normal','height': 'auto','textAlign': 'center',},
                                data=table1.to_dict('records'),
                                columns=[{'id': c, 'name': c} for c in table1.columns],
                                # fixed_rows={'headers': True},
                                #page_size= min(25, table5.shape[0]), 
                                style_as_list_view=True,
                                style_table={'height': '300px', 'overflowY': 'auto'},
                                style_data={
                                'backgroundColor': 'rgb(50, 50, 50)',
                                'color': 'white'},
                                # page_action="native",
                                # filter_action="native",
                                # sort_action="native",
                                # sort_mode="multi",
                                )
                         ], width=3),], align='center'), 
            html.Br(),

                    


    

    
])

# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
     Output('barChart', 'figure'),
     Input('keyword_filter','value'))

def display_Exp(keyword_filter):
    
    #* Chart 2
    if keyword_filter == "none":
        data = df.loc[:,"ZERO":'FIFTEEN'].astype('int64').sum()
    else:
        data = df.loc[df[keyword_filter]==1,"1":'15'].sum()
    chart2 = px.bar(data, x=data.index, y=data,
                labels={'index':'Years of experience required','y': 'No. of jobs'})
    return chart2

# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True)