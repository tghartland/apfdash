import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt

import plotly.plotly as py
import plotly.graph_objs as go

import pandas as pd

from app import app

from datasources import Datasources


def generate_table(search_term, dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr(
            [html.Td(dcc.Link(dataframe.iloc[i][0], href="/queue/{}".format(dataframe.iloc[i][0])))] + [html.Td(dataframe.iloc[i][col]) for col in dataframe.columns[1:]]
        ) for i in range(0, len(dataframe)) if search_term.lower() in dataframe.iloc[i][0].lower()][0:max_rows],
        
        id="queue-table"
    )


def generate_datatable():
    #dataframe = Datasources.get_latest_data_for("aws-athena-query-results-lancs-30d")
    #dataframe.loc[0:20]
    #dataframe = dataframe[dataframe["match_apf_queue"] == "NCG-INGRID-PT_MCORE-7435"]
    DF_GAPMINDER = pd.read_csv(
        'https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv'
    )
    DF_GAPMINDER = DF_GAPMINDER[DF_GAPMINDER['year'] == 2007]
    DF_GAPMINDER.loc[0:20]
    print("Creating table")
    #table = dt.DataTable(
    #    rows=[{}], #dataframe.to_dict("records"),
        #columns=dataframe.columns,
        #filterable=True,
        #sortable=True,
    #)
    #table = dt.DataTable(
    #    # Initialise the rows
    #    rows=DF_GAPMINDER.to_dict("records"),
    #    row_selectable=True,
    #    filterable=True,
    #    sortable=True,
    #    selected_row_indices=[],
    #    id='table123',
    #)
    table = dt.DataTable(
        rows=DF_GAPMINDER.to_dict('records'),

        # optional - sets the order of columns
        columns=sorted(DF_GAPMINDER.columns),

        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-gapminder'
    )
    #print("Returning table")
    #print()
    #print(table)
    #print()
    return table


def generate_plot(dataframe, limit=10, search_term=None):
    dataframe.insert(5, "long_jobs", dataframe["total_jobs"]-dataframe["short_jobs"])
    if not search_term is None:
        if len(search_term) > 0:
            dataframe = dataframe[dataframe["match_apf_queue"].str.contains(search_term, case=False)]
    
    trace1 = go.Bar(
        y=dataframe["match_apf_queue"][0:limit][::-1],
        x=dataframe["short_jobs"][0:limit][::-1],
        name="Short jobs",
        orientation="h",
        marker=dict(
            color="#C21E29",
        ),
    )
    
    trace2 = go.Bar(
        y=dataframe["match_apf_queue"][0:limit][::-1],
        x=dataframe["long_jobs"][0:limit][::-1],
        name="Long jobs",
        orientation="h",
        marker=dict(
            color="#3A6CAC",
        ),
    )
    
    data = [trace1, trace2]
    layout = go.Layout(
        title="Top 10 queues by most short jobs (30 days)",
        barmode="stack",
        margin=dict(
            l=275,
        ),
        xaxis = go.XAxis(
            title="Jobs",
            fixedrange=True,
        ),
        yaxis = go.YAxis(
            fixedrange=True,
        ),
    )
    return {
        "data": data,
        "layout": layout,
    }


def generate_layout():
    layout = html.Div(
        [
            html.Div([
                #html.H4("Queue comparison", id="title"),
                html.Div(style={"width":"100%", "height":"1", "overflow":"hidden",}),
                dcc.Graph(id="queue-comparison", figure={}, config={'displayModeBar': False}),
                ],
                style=dict(
                    width="60%",
                    display="inline-block",
                ),
            ),
            html.Div([
                html.Div([
                    dcc.Input(id='queue-search', value='', type="text", style={"display":"inline-block"}),
                    html.Div(" ", id="table-search-feedback", style={"display":"inline-block", "margin-left": 10})
                ]),
                generate_table(" ", Datasources.get_latest_data_for("aws-athena-query-results-lancs-30d"), 10),
                ],
                style=dict(
                    width="40%",
                    display="inline-block",
                ),
            ),
            html.Div("hi"),
            generate_datatable(),
        ],
    )
    #print(layout)    
    return layout



@app.callback(
    Output("queue-comparison", "figure"),
    [Input(component_id='queue-search', component_property='value')]
)
def update_plot(input_value):
    return generate_plot(Datasources.get_latest_data_for("aws-athena-query-results-lancs-30d"),
        search_term=input_value)


@app.callback(
    Output(component_id='queue-table', component_property='children'),
    [Input(component_id='queue-search', component_property='value')]
)
def update_output_div(input_value):
    return generate_table(input_value, Datasources.get_latest_data_for("aws-athena-query-results-lancs-30d"))


@app.callback(
    Output("table-search-feedback", "children"),
    [Input("queue-search", "value")]
)
def update_search_feedback(input_value):
    df = Datasources.get_latest_data_for("aws-athena-query-results-lancs-30d")
    matching_results = len([q for q in df["match_apf_queue"] if input_value.lower() in q.lower()])
    showing, hidden = min(matching_results, 10), max(0, matching_results-10)
    
    return "Showing {} results ({} hidden)".format(showing, hidden)

if __name__ == '__main__':
    app.run_server()
