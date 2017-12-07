from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

from werkzeug.routing import Map, Rule
from werkzeug.routing import NotFound, RequestRedirect

import datasources

routing_map = Map([
    Rule("/", endpoint="index"),
    Rule("/queue/<string:queue_name>/", endpoint="queue"),
    Rule("/debug/", endpoint="debug")
])

routes = routing_map.bind("")

from app import app
from apps import index_app
from apps import queue_app
from apps import debug_app

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


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
    
    if endpoint == "index":
        return index_app.layout
    elif endpoint == "queue":
        return queue_app.generate_layout(args.get("queue_name"))
    elif endpoint == "debug":
        return debug_app.generate_layout()

if __name__ == '__main__':
    app.run_server(debug=True)
