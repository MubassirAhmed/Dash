import pandas as pd
import plotly.express as px 
import os
import snowflake.connector


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


if __name__ == '__main__':
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

    data2 = df.loc[:,"ONE":'FIFTEEN'].sum()
    print(data2)
    chart2 = px.bar(data2, x=data2.index, y=data2,
                labels={'index':'Years of experience required','y': 'No. of jobs'})

    """data = df.loc[:,"ONE":'FIFTEEN'].sum()
    chart2 = px.bar(data, x=data.index, y=data,
                    labels={'index':'Years of experience required','y': 'No. of jobs'})"""
                    
        # Iris bar figure
def drawFigure():
    return  html.Div([
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(
                    figure=px.bar(
                        df, x="sepal_width", y="sepal_length", color="species"
                    ).update_layout(
                        template='plotly_dark',
                        plot_bgcolor= 'rgba(0, 0, 0, 0)',
                        paper_bgcolor= 'rgba(0, 0, 0, 0)',
                    ),
                    config={
                        'displayModeBar': False
                    }
                ) 
            ])
        ),  
    ])
    
                html.Div(
                dbc.Card(
                    dbc.CardBody([
                    ])
                )
            ),

# Text field
def drawText():
    return html.Div([
            dbc.Card(
                dbc.CardBody([html.Div(
                            [html.H2("Text"),],
                            style={'textAlign': 'center'}
                            ) 
                        ])
                ),
     ])            
                    
                    

    dbc.Card(
        dbc.CardBody([
            
            dbc.Row([
                dbc.Col([drawText()], width=3),
                dbc.Col([drawText()], width=3),
                dbc.Col([drawText()], width=3),
                dbc.Col([drawText()], width=3),
                ], align='center'), 
            
            html.Br(),
            
            dbc.Row([
                dbc.Col([drawFigure() ], width=3),
                dbc.Col([drawFigure()], width=3),
                dbc.Col([drawFigure() ], width=6),
                ], align='center'), 
            
            html.Br(),
            
            dbc.Row([dbc.Col([drawFigure()], width=9),
                dbc.Col([drawFigure()], width=3),
                ], align='center'),      
        ]), 
        
        color = 'dark'
    ),