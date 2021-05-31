import orjson
from dash import dash
import dash_bootstrap_components as dbc

import sys
import os
d = os.popen("cd ../../core/src && pwd").read()[:-1]
sys.path.append(d)

external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    "../spc-custom-styles.css",
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
data = orjson.loads(open("/usr/local/share/spark-luxmeter/config.json", "r").read())
