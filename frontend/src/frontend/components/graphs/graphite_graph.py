import dash_core_components as dcc
import plotly.graph_objs as go
from dash.dependencies import Output, Input
from dash.exceptions import PreventUpdate

from frontend import graphitestore
from frontend.components import Component

V = "1"


class GraphiteGraphComponent(Component):
    id = "base-graphite-graph"

    def __init__(self):
        self.metrics_keys = ["anomaly_detection.sequential.%s.{app}.*.*" % V]

    def render(self):
        figure = go.Figure()
        return dcc.Graph(id=self.id, figure=figure)

    def _compose_figure(self, metrics, timestamps):
        fig = go.Figure()
        for key, values in metrics.items():
            timestamps_filtered = [
                t for t, v in zip(timestamps, values) if v is not None
            ]
            value_filtered = [v for v in values if v is not None]
            key_split = key.split(".")
            fig.add_trace(
                go.Scatter(
                    x=timestamps_filtered,
                    y=value_filtered,
                    name=f"{key_split[4]}:{key_split[5]}",
                    mode="lines",
                )
            )
        return fig

    def _figure_from_metrics(self, metrics_keys, start_dt, end_dt):
        return self._compose_figure(
            *graphitestore.client.load(metrics_keys, since=start_dt, until=end_dt)
        )

    def add_callbacks(self, app):
        @app.callback(
            Output(self.id, "figure"),
            Input("plots-time-range", "data"),
            Input("interval", "n_intervals"),
            Input("selected-app-info", "data"),
            Input(self.id, "figure"),
        )
        def update_figure(dt_range, n, selected_app, last_figure):
            start_dt, end_dt = dt_range
            if not selected_app:
                raise PreventUpdate

            app_id = selected_app["app_id"]
            new_fig = self._figure_from_metrics(
                list(map(lambda x: x.format(app=app_id), self.metrics_keys)),
                start_dt,
                end_dt,
            )

            new_fig["layout"]["xaxis"] = last_figure["layout"]["xaxis"]
            new_fig["layout"]["yaxis"] = last_figure["layout"]["yaxis"]

            # new_fig = self._figure_from_metrics(list(map(lambda x: x.format(app=selected_app["app_id"]), self.metrics_keys)))
            return new_fig
