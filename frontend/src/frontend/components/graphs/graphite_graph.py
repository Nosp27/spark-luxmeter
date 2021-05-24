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
        self.metrics_keys = ["anomaly_detection.sequential.%s.{app}.*.target" % V]

    def render(self):
        figure = go.Figure()
        return dcc.Graph(id=self.id, figure=figure)

    def _compose_figure(self, metrics, timestamps):
        fig = go.Figure()
        for key, values in metrics.items():
            fig.add_trace(go.Scatter(x=timestamps, y=values, name=key, mode="lines"))
        return fig

    def _figure_from_metrics(self, metrics_keys):
        return self._compose_figure(*graphitestore.client.load(metrics_keys))

    def add_callbacks(self, app):
        @app.callback(
            Output(self.id, "figure"),
            Input("interval", "n_intervals"),
            Input("selected-app-info", "data"),
            Input(self.id, "figure"),
        )
        def update_figure(n, selected_app, last_figure):
            if not selected_app:
                raise PreventUpdate

            new_fig = self._figure_from_metrics(["stats_counts.response.200"])

            new_fig["layout"]["xaxis"] = last_figure["layout"]["xaxis"]
            new_fig["layout"]["yaxis"] = last_figure["layout"]["yaxis"]

            # new_fig = self._figure_from_metrics(list(map(lambda x: x.format(app=selected_app["app_id"]), self.metrics_keys)))
            return new_fig
