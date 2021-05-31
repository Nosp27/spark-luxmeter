import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import orjson
import requests
from dash.dependencies import Output, Input, State
from dash.exceptions import PreventUpdate

from frontend import kvstore
from frontend.components.abc import Component
from frontend.tools import format_app
from spark_logs import kvstore as kv_keys


class TaskList(Component):
    def __init__(self):
        self.tasks = None

    def generate_sample_data(self, size=10):
        return []

    def render(self):
        return html.Div(
            children=[
                html.Div(
                    style=dict(
                        display="flex", justifyContent="flex-start", alignItems="center"
                    ),
                    children=[
                        html.P("Applications", style=dict(margin="10px"),),
                        dcc.Dropdown(
                            style=dict(width="700px"),
                            id="app-list",
                            options=[],
                            searchable=True,
                            clearable=False,
                        ),
                        dbc.Alert(
                            id="app-list-alert",
                            style=dict(margin="15px"),
                            color="danger",
                            is_open=False,
                        ),
                    ],
                ),
            ]
        )

    def add_callbacks(self, app):
        @app.callback(
            Output("app-list-info", "data"),
            Output("app-list-alert", "children"),
            Output("app-list-alert", "is_open"),
            Input("interval", "n_intervals"),
            Input("hostname-info", "data"),
        )
        def load_app_list(_, hostname_info):
            if not hostname_info:
                return [], "", False
            apps = dict()
            try:
                apps = orjson.loads(kvstore.client.get(kv_keys.applications_key()))
            except Exception as exc:
                # Alert
                alert_mode = True
                alert_text = str(exc)
            else:
                alert_mode = False
                alert_text = ""
            return apps, alert_text, alert_mode

        @app.callback(
            Output("app-list", "options"),
            Input("processing-applications-ids", "data"),
            State("app-list-info", "data"),
        )
        def render_app_list(processing_app_ids, app_list_info):
            if not processing_app_ids or not app_list_info:
                raise PreventUpdate

            return [
                dict(label=format_app(app_list_info[x]), value=x,)
                for x in processing_app_ids
            ]

        # Select applications
        @app.callback(
            Output("selected-app-info", "data"),
            Input("app-list", "value"),
            prevent_initial_call=True,
        )
        def select_application(app_id):
            try:
                raise requests.RequestException()
                # hybrid_metrics_json = kvstore.client.zrevrangebyscore(
                #     f"hm:{app_id}:{job_id}:{metric_name}"
                # )
            except requests.RequestException:
                hybrid_metrics_json = None

            try:
                raw = kvstore.client.get(kv_keys.app_environment_key(app_id=app_id))
                if not raw:
                    environment = None
                else:
                    environment = orjson.loads(raw)
            except requests.exceptions.RequestException:
                environment = None

            return dict(
                app_id=app_id,
                hybrid_metrics_json=hybrid_metrics_json,
                environment=environment,
            )
