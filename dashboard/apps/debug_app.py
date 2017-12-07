import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import humanize
from datetime import datetime
from dateutil.tz import tzutc

from app import app

from datasources import Datasources, QueryHistory


def debug_table():
    column_names = ["Bucket", "Latest object", "Object modified", "Downloaded", "Checked for update"]

    rows = [html.Tr([html.Td(n) for n in column_names])]
    
    for bucket_name, obj in Datasources.query_data.items():
        rows.append(html.Tr([
            html.Td(bucket_name),
            html.Td(obj["filename"]),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["modified"])),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["downloaded"])),
            html.Td(humanize.naturaltime(datetime.now(tzutc())-obj["checked_for_update"])),
        ]))

    return html.Table(rows)


def debug_query_history():
    column_names = ["Query name", "Query ID", "Result file", "Execution time", "Status"]
    
    rows = [html.Tr([html.Td(n) for n in column_names])]
    
    for event in reversed(QueryHistory.history):
        rows.append(html.Tr(
            [html.Td(str(x)) for x in event]
            )
        )
    print(rows)
    return html.Table(rows)

def generate_layout():
    layout = [
        dcc.Link("Index", href="/"),
        html.Div([
            html.H4("Current data"),
            debug_table(),
        ], style={"margin-bottom":"100px"}),
        html.Div([
            html.H4("Query history"),
            debug_query_history(),
        ]),
    ]
    return layout
