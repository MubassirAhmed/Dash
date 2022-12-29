import pandas as pd
import plotly.express as px  # (version 4.7.0 or higher)
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output  # pip install dash (version 2.0.0 or higher)
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



#* chart 2
#so for this i need a column 'Years' with numbers from 1 to 15, and I group by 'Years' to aggregate on jobs. This dimension will also have a YoEid that will link it to the main table.
#TODO I dont need this. I can just aggregate on the current years tables that I have. But it's highly inefficient to have so many columns with nulls when they don't need to exist. 
def chart2():
    
    con = get_snowflake_connector()
    cur =  con.cursor()
    #? write a query such that you get aggregate over the years columns, which you will get from transform2.py. Just hardcode those and 
    query= '''
    select
        count(distint job_id),
        year
        group by year
        having year = 1;
    
    show how many jobs there are for every hour of the day data collected
    select 
        TIMESTAMP
    '''
    cur.execute(query)
    df = cur.fetch_pandas_all()

    fig = px.bar(df, x="Years of Exp. Required", y="No. of Jobs")



def plotChart1():
    con = get_snowflake_connector()
    cur =  con.cursor()
    
    #! I am postponing this for now as I don't have enough data yet. I need atleast 1 day of full data for this to work, preferably 2
    query= '''
    show how many jobs there are for every hour of the day data collected
    select 
        TIMESTAMP
    '''
    cur.execute(query)
    
    df = cur.fetch_pandas_all()
    
    #chart 1
    #so there should be a column named Time of Day and a column named No. of Jobs Posted
    timeline = ['Time of Day', 'Day of Week', "Day of Month", 'Month']
    fig = px.bar(df, x="Time of Day", y="No. of Jobs Posted") #, color="City", barmode="group")

def main():
    plotChart1()
    pass


if __name__ == '__main__':
    main()