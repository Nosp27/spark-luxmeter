from typing import Optional

import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from frontend.components.abc import Component
from spark_logs.types import ApplicationMetrics


class AppSummary(Component):
    def __init__(self, uid):
        self.uid = uid
        self.task_state = dict(
            cpu_utilization=0,
            shuffle=0,
            # runtime=timedelta(hours=3, minutes=14, seconds=38),
        )

    @property
    def container_id(self):
        return self.uid + "-container"

    def random_data(self):
        pass

    def random_plot(self):
        pass

    def render(self):
        return dbc.Container(
            id=self.container_id,
            children=[
                dbc.Row(
                    children=[
                        dbc.Col(dbc.Card([html.H2(f"{k}"), html.P(v)], body=True))
                        for k, v in self.task_state.items()
                    ]
                ),
            ],
            fluid=False,
        )

    def extract_app_state(self, loaded_json):
        loaded = ApplicationMetrics.create_from_dict(loaded_json)
        executor = loaded.executor_metrics[1]
        return {
            "Cores Used": executor.totalCores,
            "Shuffle Read": executor.totalShuffleRead,
            "Shuffle Write": executor.totalShuffleWrite,
        }

    def add_callbacks(self, app):
        @app.callback(
            Output(self.container_id, "children"), Input("selected-app-info", "data"),
        )
        def update_state(selected_app_info):
            loaded_json: Optional[ApplicationMetrics] = selected_app_info.get(
                "app_metrics_json"
            )
            if not loaded_json:
                raise PreventUpdate
            self.task_state = self.extract_app_state(loaded_json)
            return self.render()
