from collections import defaultdict

import dash
import dash_core_components as dcc
import numpy as np
import plotly.graph_objs as go
from dash.dependencies import Output, Input
from dash.exceptions import PreventUpdate

from frontend import graphitestore, kvstore
from frontend.components import Component

V = "1"


class GraphiteGraphComponent(Component):
    id = "base-graphite-graph"
    metrics_keys = []

    def _compose_figure(self, metrics, timestamps, **kwargs):
        fig = go.Figure(
            layout=go.Layout(
                title=go.layout.Title(text="Anomaly detection raw metrics")
            )
        )
        for key, values in metrics.items():
            timestamps_filtered = [
                t for t, v in zip(timestamps, values) if v is not None
            ]
            value_filtered = [v for v in values if v is not None]
            fig.add_trace(
                go.Scatter(
                    x=timestamps_filtered,
                    y=value_filtered,
                    name=self.legend_name(key),
                    mode="lines+markers",
                )
            )
        return fig

    def render(self):
        figure = go.Figure()
        return dcc.Graph(id=self.id, figure=figure)

    def legend_name(self, key):
        key_split = key.split(".")
        return f"{key_split[4]}:{key_split[5]}"

    def format_metrics_keys(self, keys, selected_app):
        return list(map(lambda x: x.format(app=selected_app["app_id"]), self.metrics_keys))

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

            new_fig = self._figure_from_metrics(
                self.format_metrics_keys(self.metrics_keys, selected_app),
                start_dt,
                end_dt,
            )

            new_fig["layout"]["xaxis"] = last_figure["layout"]["xaxis"]
            new_fig["layout"]["yaxis"] = last_figure["layout"]["yaxis"]
            return new_fig


class AnomalyGraphiteGraphComponent(GraphiteGraphComponent):
    id = "anomaly-graphite-graph"

    metrics_keys = ["anomaly_detection.sequential.%s.{app}.*.*" % V]

    def legend_name(self, key):
        key_split = key.split(".")
        return f"{key_split[4]}:{key_split[5]}"


class DifferGraphiteGraphComponent(GraphiteGraphComponent):
    id = "differ-graphite-graph"

    metrics_keys = [
        "anomaly_detection.sequential.%s.{app}.*.*" % V,
    ]

    def legend_name(self, key):
        key_split = key.split(".")
        return f"{key_split[4]}:{key_split[5]}"

    def maxline(self, app_id, jg):
        maxline_data = graphitestore.client.load(
            [
                f"aliasByNode(absolute(diffSeries(anomaly_detection.sequential.{V}.{app_id}.{jg}.target, "
                f"anomaly_detection.sequential.{V}.{app_id}.{jg}.predict)), 4, 5)"
            ],
            since="now-11h",
            until="now-1h",
        )
        lst = [x for x in list(maxline_data[0].values())[0] if x is not None]
        if not lst:
            return None
        return np.percentile(lst, 95)

    def _figure_from_metrics(self, metrics_keys, app_id, start_dt, end_dt):
        return self._compose_figure(
            *graphitestore.client.load(metrics_keys, since=start_dt, until=end_dt),
            app_id=app_id,
        )

    def _compose_figure(self, metrics, timestamps, *, app_id, **kwargs):
        colors = ["darkseagreen", "darksalmon", "violet"]
        fig = go.Figure(
            layout=go.Layout(
                title=go.layout.Title(text="Anomaly Score")
            )
        )

        groups = defaultdict(list)
        for k, vs in metrics.items():
            groups[k.split(".")[4]].append(vs)

        group_colors = dict(zip(groups.keys(), colors))

        for group_name, group_data in groups.items():
            deltas = [abs(x - y) if None not in (x, y) else None for x, y in zip(*group_data)]
            maxline = self.maxline(app_id, group_name)

            timestamps_filtered = [
                t for t, v in zip(timestamps, deltas) if v is not None
            ]
            deltas_filtered = [v for v in deltas if v is not None]

            fig.add_trace(
                go.Scatter(
                    x=timestamps_filtered,
                    y=deltas_filtered,
                    name=group_name,
                    marker_color=group_colors[group_name],
                    mode="lines+markers",
                )
            )

            if maxline:
                fig.add_trace(
                    go.Scatter(
                        x=timestamps_filtered,
                        y=[maxline] * len(timestamps_filtered),
                        marker_color=group_colors[group_name],
                        mode="lines+markers",
                        line=dict(dash="dash"),
                    )
                )
        return fig

    def get_dt_range(self, timerange_raw):
        pass

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

            new_fig = self._figure_from_metrics(
                self.format_metrics_keys(self.metrics_keys, selected_app),
                selected_app["app_id"],
                start_dt,
                end_dt,
            )

            new_fig["layout"]["xaxis"] = last_figure["layout"]["xaxis"]
            new_fig["layout"]["yaxis"] = last_figure["layout"]["yaxis"]
            return new_fig
