import random
from datetime import datetime, timedelta

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import orjson
import requests
from dash.dependencies import Output, Input, State
from dash.exceptions import PreventUpdate

from frontend import kvstore
from frontend.components import rest_api
from frontend.components.abc import Component


class TaskList(Component):
    def __init__(self):
        self.tasks = None

    def generate_sample_data(self, size=10):
        return [
            (
                f"Task_{i}",
                f"task_{i}",
                "nosp27",
                datetime.now() - timedelta(seconds=random.randint(120, 600)),
            )
            for i in range(size)
        ]

    def render(self, tasks):
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
                            options=[
                                dict(
                                    label=f"{task_title} by {task_owner} since {task_start_time}",
                                    value=task_id,
                                    title=task_id,
                                )
                                for task_title, task_id, task_owner, task_start_time in tasks
                            ],
                            searchable=True,
                            clearable=False,
                            value=tasks[0][1],
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
            State("hostname-info", "data"),
        )
        def load_app_list(_, hostname_info):
            if not hostname_info:
                return [], "", False
            apps = []
            try:
                apps_raw = kvstore.client.zrangebyscore(
                    "applications", min=-float("inf"), max=float("inf")
                )
                apps = [a.decode() for a in apps_raw]
            except Exception as exc:
                # Alert
                alert_mode = True
                alert_text = str(exc)
            else:
                alert_mode = False
                alert_text = ""
            return apps, alert_text, alert_mode

        @app.callback(
            Output("app-list", "options"), Input("app-list-info", "data"),
        )
        def render_app_list(app_list_info):
            if not app_list_info:
                raise PreventUpdate
            return [
                dict(label=f"{x} by since", value=x, title=x,) for x in app_list_info
            ]

        # Select applications
        @app.callback(
            Output("selected-app-info", "data"),
            Input("app-list", "value"),
            prevent_initial_call=True,
        )
        def select_application(app_id):
            try:
                app_metrics_json = kvstore.client.zrevrangebyscore(
                    app_id, min="-inf", max="+inf", num=1, start=0
                )
            except requests.RequestException:
                app_metrics_json = None
            try:
                raise requests.RequestException()
                # hybrid_metrics_json = kvstore.client.zrevrangebyscore(
                #     f"hm:{app_id}:{job_id}:{metric_name}"
                # )
            except requests.RequestException:
                hybrid_metrics_json = None
            if app_metrics_json:
                app_metrics_json = orjson.loads(app_metrics_json[0])
            if hybrid_metrics_json:
                hybrid_metrics_json = None
            return dict(
                app_id=app_id,
                app_metrics_json=app_metrics_json,
                hybrid_metrics_json=hybrid_metrics_json,
            )
