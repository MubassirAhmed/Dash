import pandas as pd
import plotly.express as px  # (version 4.7.0 or higher)
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output  # pip install dash (version 2.0.0 or higher)


app = Dash(__name__)
application = app.server

# -- Import and clean data (importing csv into pandas)
# df = pd.read_csv("intro_bees.csv")
df = pd.read_csv("/Users/mvbasxhr/Cool Stuff/LinkedInScraper/linkedin/dash_testing.csv")

"""df = df.groupby(['State', 'ANSI', 'Affected by', 'Year', 'state_code'])[['Pct of Colonies Impacted']].mean()
df.reset_index(inplace=True)
print(df[:5])"""

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H4("Demand for Years of Experience", style={'text-align': 'center'}),
    dcc.Graph(id='barChart'),
    dcc.Dropdown(
        id ='keyword_filter',
        options = {
            'sql':'SQL',
            'python':'Python'
        },
        value='none'
    )
])


# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
     Output('barChart', 'figure'),
     Input('keyword_filter','value'))

def display_Exp(keyword_filter):
    if keyword_filter == "none":
        data = df.loc[:,"1":'15'].sum()
    else:
        data = df.loc[df[keyword_filter]==1,"1":'15'].sum()
    fig = px.bar(data,
                x=data.index,
                y=data,
                labels={'index':'Years of Experience','y': 'Frequency'})
    
    return fig

# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True)