import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt


import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import numpy as np
import plotly


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
    dataframe = Datasources.get_latest_data_for("aws-athena-apfdash-index")
    dataframe["empty_pcent"] = (100*dataframe["empty_jobs"]/dataframe["total_jobs"]).round(2)
    dataframe["emptywc_pcent"] = (100*dataframe["remotewallclocktime_empty"]/dataframe["remotewallclocktime"]).round(2)
    
    
    dataframe = dataframe[["match_apf_queue", "total_jobs", "empty_jobs", "empty3_jobs", "empty4_jobs", "empty_pcent", "remotewallclocktime", "remotewallclocktime_empty", "emptywc_pcent"]]
    #dataframe.set_index(["Queue", "Jobs", "Empty jobs", "% Empty", "Wallclock", "Wallclock (empty)", "Wallclock (% empty)"])
    dataframe = dataframe.rename(columns={
        "match_apf_queue": "Queue",
        "total_jobs": "Jobs",
        "empty_jobs": "Jobs (empty)",
        "empty3_jobs": "Jobs (empty 3)",
        "empty4_jobs": "Jobs (empty 4)",
        "empty_pcent": "% Empty",
        "remotewallclocktime": "Wallclock (total)",
        "remotewallclocktime_empty": "Wallclock (empty)",
        "emptywc_pcent": "Wallclock (% empty)",
    })
    # records = dataframe.to_dict("records")
    # for i, record in enumerate(records):
    #     record["match_apf_queue"] = dcc.Link(record["match_apf_queue"], href="/queue/{}".format(record["match_apf_queue"]))
    #     records[i] = record
    #
    # print(records)
    
    table = dt.DataTable(
        rows=dataframe.to_dict("records"),
        columns=dataframe.columns,
        filterable=True,
        sortable=True,
        editable=False,
        id="queue-datatable",
        min_height=800,
    )
    
    return table

@app.callback(
    Output("queue-comparison", "figure"),
    [Input(component_id='queue-datatable', component_property='rows'),
     Input("num-queues-dropdown", "value")],
)
def update_plot(rows, limit):
    selected_rows = [row["Queue"] for row in rows]
    return generate_plot(Datasources.get_latest_data_for("aws-athena-apfdash-index"),
        filtered_by=selected_rows, limit=limit)


def generate_plot(dataframe, limit=10, search_term=None, filtered_by=None):
    dataframe.insert(5, "long_jobs", dataframe["total_jobs"]-dataframe["empty_jobs"])
    """if not search_term is None:
        if len(search_term) > 0:
            dataframe = dataframe[dataframe["match_apf_queue"].str.contains(search_term, case=False)]"""
    
    dataframe = dataframe[dataframe["match_apf_queue"].isin(filtered_by)]
    dataframe.insert(len(dataframe.columns), "order", dataframe.loc[:, "match_apf_queue"].apply(lambda x: filtered_by.index(x)))
    dataframe = dataframe.sort_values(by="order")
    dataframe.drop("order", axis=1)
    
    dataframe["match_apf_queue"] = dataframe["match_apf_queue"].apply(lambda x: "<a href=\"/queue/{0}\">{0}</a>".format(x))
    
    trace1 = go.Bar(
        y=dataframe["match_apf_queue"][0:limit][::-1],
        x=dataframe["empty4_jobs"][0:limit][::-1],
        name="Empty (4)",
        orientation="h",
        marker=dict(
            color="#C21E29",
        ),
    )
    
    trace2 = go.Bar(
        y=dataframe["match_apf_queue"][0:limit][::-1],
        x=dataframe["empty3_jobs"][0:limit][::-1],
        name="Empty (3)",
        orientation="h",
        marker=dict(
            color="#EF751A",
        ),
    )
    
    trace3 = go.Bar(
        y=dataframe["match_apf_queue"][0:limit][::-1],
        x=dataframe["long_jobs"][0:limit][::-1],
        name="Payload",
        orientation="h",
        marker=dict(
            color="#3A6CAC",
        ),
    )
    
    data = [trace1, trace2, trace3]
    layout = go.Layout(
        title="Queue comparison (past 24 hours)",
        barmode="stack",
        margin=dict(
            t=30,
            l=275,
            b=40,
        ),
        xaxis = go.XAxis(
            title="Jobs",
            fixedrange=True,
        ),
        yaxis = go.YAxis(
            fixedrange=True,
        ),
        height=int(70+27*min(limit, len(dataframe))),
    )
    return {
        "data": data,
        "layout": layout,
    }

help_panel = [
    html.P("The data table can be sorted and filtered."),
    html.P("Queue names in the comparison plot are clickable links."),
]

def generate_layout():
    layout = [
        html.Div([
            html.Div([
                html.H4("Queue comparison", id="title"),
                html.Div([
                    html.Div([html.Label('# queues displayed'),], style={"float":"left"}),
                    html.Div([
                        dcc.Dropdown(
                            options=[
                                {"label":"10", "value":10},
                                {"label":"25", "value":25},
                                {"label":"50", "value":50},
                            ],
                            value=25,
                            clearable=False,
                            id="num-queues-dropdown",
                        ),
                    ], style={"width": "80px", "display":"inline-block"})
                ]),
                dcc.Graph(id="queue-comparison", figure={}, config={'displayModeBar': False}),
                ],
                className="index-grid-left",
                # style=dict(
                #     width="50%",
                #     float="left",
                # ),
            ),
            html.Div([
                    generate_datatable(),
                ],
                className="index-grid-right",
                # style={
                #     "width":"50%",
                #     "float":"right",
                #     "height":"100%",
                #     "min-height":"500px",
                #     "display":"table",
                # }
            ),
            
        ],
        className="index-grid-container",
        # style={
        #     "height":"100%",
        #     "display":"flex",
        # }
        )
    ]
    
    return help_panel, layout
