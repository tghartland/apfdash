import os.path

import dash
import flask

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
CSS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'css')

app = dash.Dash()
server = app.server
app.config.suppress_callback_exceptions = True
# app.scripts.config.serve_locally = True

@app.server.route('/scripts/<resource>')
def serve_script(resource):
    return flask.send_from_directory(SCRIPT_PATH, resource)

@app.server.route('/css/<resource>')
def serve_css(resource):
    return flask.send_from_directory(CSS_PATH, resource)

# jQuery
app.scripts.append_script({"external_url": "https://code.jquery.com/jquery-3.2.1.slim.min.js"})

# for bootstrap
#app.scripts.append_script({"external_url": "https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js"})

# used for selecting elements in page as they are initialised
app.scripts.append_script({"external_url": "https://rawgit.com/pie6k/jquery.initialize/master/jquery.initialize.min.js"})

app.scripts.append_script({"external_url": "/scripts/fix_datatable.js"})

# dash example css and own css overrides
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

# custom bootstrap css/js with only panels enabled
# from https://getbootstrap.com/docs/3.3/customize
app.scripts.append_script({"external_url": "/scripts/bootstrap.js"})
app.css.append_css({"external_url": "/css/bootstrap.css"})

# my css is loaded last so that it has the final say on overriding things
app.css.append_css({"external_url": "/css/index.css"})