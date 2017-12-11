import dash

app = dash.Dash()
server = app.server
app.config.suppress_callback_exceptions = True
# app.scripts.config.serve_locally = True

app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

app.scripts.append_script({"external_url": "https://code.jquery.com/jquery-3.2.1.min.js"})
app.scripts.append_script({"external_url": "https://rawgit.com/pie6k/jquery.initialize/master/jquery.initialize.min.js"})

# Load js script via rawgit
#  - github prevents direct loading of js scripts by setting the MIME type to text/plain
# Trying to host the file locally is a pain because dash intercepts all routes and tries to return a dashboard for them (which fails)
app.scripts.append_script({"external_url": "https://rawgit.com/H4rtland/athena_project/master/dashboard/scripts/fix_datatable.js"})