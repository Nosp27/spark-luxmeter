import itertools
import random
from datetime import datetime, timedelta

import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import requests
from dash.dependencies import Output, Input, State
from dash.exceptions import PreventUpdate

from frontend import graphitestore, kvstore

from frontend.components.abc import Component


class MemoryPlot(Component):
    mem_keys_mapping = {
        "memoryUsed": "MiscMemoryUse",
    }

    def __init__(self):
        self.figure = None

    def render(self):
        return self.compose_layout(dict(), [])

    def compose_layout(self, timestamps, values):
        self.figure = self.compose_figure(timestamps, values)
        return html.Div(children=dcc.Graph(id="mem-plot", figure=self.figure))

    def compose_figure(self, values, timestamps):
        # assert all(k in values for k in self.mem_keys)
        delays = [
            (timestamps[i + 1] - timestamps[i]).total_seconds() * 1000
            for i in range(len(timestamps) - 1)
        ]
        fig = go.Figure()
        colors = {
            "Free": "lightgray",
            "memoryUsed": "rosybrown",
        }
        for key, value in values.items():
            timestamps_filtered = [
                t for t, v in zip(timestamps, value) if v is not None
            ]
            value_filtered = [v for v in value if v is not None]
            fig.add_bar(
                x=timestamps_filtered,
                y=value_filtered,
                name=key,
                width=delays,
                marker_color=colors[key],
            )
        fig.update_layout(
            barmode="relative",
            title_text="Memory consumption",
            transition_duration=500,
        )
        fig.update_yaxes(type="log")
        return fig

    def add_callbacks(self, app):
        @app.callback(
            Output("mem-plot", "figure"),
            Input("selected-app-info", "data"),
            Input("interval", "n_intervals"),
            State("plots-time-range", "data"),
        )
        def update_memplot(selected_app_data, n_intervals, time_range):
            start_dt, end_dt = time_range
            if not n_intervals or n_intervals % 4 != 0:
                raise PreventUpdate
            if not selected_app_data:
                raise PreventUpdate
            client = graphitestore.client
            app_id = selected_app_data["app_id"]
            try:
                raw_metrics, timestamps = client.load(
                    [
                        f"groupByNodes(loader.executors.{app_id}.*.memoryUsed, 'sum', 2, 4)"
                    ],
                    since=start_dt,
                    until=end_dt,
                    interpolation=False,
                )
            except requests.exceptions.RequestException as exc:
                raise PreventUpdate
            except ValueError:
                raise PreventUpdate

            environ = selected_app_data["environment"]
            if environ is None:
                raise PreventUpdate

            e_mem = environ.get("spark.executor.memory", 2 * 1024 * 1024 * 1024)
            e_oh = environ.get("spark.executor.memoryOverhead", e_mem * 0.1)
            num_inst = 1
            max_memory = (e_mem + e_oh) * num_inst

            try:
                metrics, *_ = raw_metrics.values()
            except ValueError:
                metrics = []
            metrics = [x for x in metrics]
            free_mem_metrics = [
                max_mem - used_mem if used_mem is not None else None
                for max_mem, used_mem in zip(itertools.repeat(max_memory), metrics)
            ]
            return self.compose_figure(
                {"memoryUsed": metrics, "Free": free_mem_metrics}, timestamps,
            )
