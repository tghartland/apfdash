from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt

from werkzeug.routing import Map, Rule
from werkzeug.routing import NotFound, RequestRedirect

routing_map = Map([
    Rule("/", endpoint="index"),
    Rule("/queue/<string:queue_name>/", endpoint="queue"),
    Rule("/resource/", endpoint="resource"),
    Rule("/distribution/", endpoint="distribution"),
    Rule("/debug/", endpoint="debug"),
])

routes = routing_map.bind("")

from app import app
from apps import index_app
from apps import queue_app
from apps import resource_app
from apps import debug_app
from apps import distribution_app

server = app.server

app.layout = html.Div([
    # Stores url
    dcc.Location(id='url', refresh=False),
    
    # Hidden data table to force it to send the data table js components
    # https://github.com/plotly/dash-table-experiments/issues/18
    html.Div(dt.DataTable(rows=[{}]), style={"display": "none"}),
    
    # Header bar
    html.Div([
        dcc.Link("Index", href="/", style={"margin":"5px"}),
        dcc.Link("Resource", href="/resource/", style={"margin":"5px"}),
        dcc.Link("Distribution", href="/distribution/", style={"margin":"5px"}),
        dcc.Link("Debug", href="/debug/", style={"margin":"5px"}),
        html.Hr(style={"margin": "0px", "margin-top": "2px", "margin-bottom":"10px"}),
    ], style={"width": "100%"}),
    
    # Div to load page content into
    html.Div(id='page-content', style={"width": "99%", "height":"100%", "left": "0.5%", "position": "relative"})
],)


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    while True:
        try:
            endpoint, args = routes.match(pathname)
        except RequestRedirect as redirect:
            pathname = redirect.new_url.replace("http://", "")
        except NotFound:
            return "404"
        else:
            break
    
    app_page = html.Div()
    help_panel = html.Div()
    
    if endpoint == "index":
        help_panel, app_page = index_app.generate_layout()
    elif endpoint == "queue":
        help_panel, app_page =  queue_app.generate_layout(args.get("queue_name"))
    elif endpoint == "resource":
        help_panel, app_page =  resource_app.generate_layout()
    elif endpoint == "distribution":
        help_panel, app_page =  distribution_app.generate_layout()
    elif endpoint == "debug":
        help_panel, app_page =  debug_app.generate_layout()
    
    help_box = html.Div(
        html.Div([
            html.Div(
                html.H4(
                    html.Div(
                        "Help for this page",
                        id="help-panel-collapsing-link",
                    ),
                    className="panel-title"
                ),
                className="panel-heading"
            ),
            html.Div(
                html.Div(
                    help_panel,
                    className="panel-body"
                ),
                id="collapsehelp",
                className="panel-collapse collapse"
            ),
            ],
            className="panel panel-default"
        ),
        className="panel-group"
    )
    
    return html.Div(
        [help_box]+app_page
    )

if __name__ == '__main__':
    app.run_server(debug=False)
