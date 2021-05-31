from typing import Optional

import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
from datetime import datetime
import dash_core_components as dcc
import numpy as np
from plotly.graph_objs import Figure

from frontend import kvstore, graphitestore
from dateutil import tz
from spark_logs import kvstore as kvinfo
from frontend.components.abc import Component
from spark_logs.types import ApplicationMetrics, JobStages

metric_mapping = ()


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

            ],
            fluid=True,
        )

    def render_latest_hybrid_metrics(self, app_id):
        keyspace = f"aliasByNode(groupByNode(app.{app_id}.stage.*.test.*, 3, 'avg'), 5)"
        results = graphitestore.client.load([keyspace], since="now-1h", until="now", interpolation=False)
        results = {k: next((x for x in reversed(vs) if x is not None), None) for k, vs in results}
        return results

    def render_app_info(self, app_id, environ, seq_job_data):
        if environ is None:
            raise PreventUpdate

        app_data = kvstore.client.get(app_id)
        if not app_data:
            raise PreventUpdate
        app_info = ApplicationMetrics.from_json(kvstore.client.get(app_id))
        active_executors_count = len([e for e in app_info.executor_metrics if e.isActive])

        containers = f"{active_executors_count} / {len(app_info.executor_metrics)}"
        jobs_running = f"{len([x for x in app_info.jobs_stages.values() if x.job.status == 'RUNNING'])}"

        job_start_times = np.array([x.job.submissionTime for x in seq_job_data])
        job_endtimes = np.array([x.job.completionTime for x in seq_job_data])
        job_durations = job_endtimes - job_start_times

        levels = (5, 75, 95)
        perc_values = [np.percentile(job_durations, level) for level in levels]
        perc_table = html.Table(
            [
                html.Tr(
                    [html.Th(str(level)) for level in levels]
                ),
                html.Tr(
                    [html.Td(str(v)) for v in perc_values]
                )
            ]
        )

        fig = Figure(
            data={
                "x": list(range(len(job_durations))),
                "y": [x.seconds for x in sorted(job_durations)],
            }
        )
        perc_graph = dcc.Graph(figure=fig)
        p_data = html.Div([perc_table, perc_graph])

        common_style = {"width": "180px", "margin": "6px"}

        wide_style = common_style.copy()
        wide_style["width"] = "500px"

        job_id = seq_job_data[-1].job.jobId
        scores = graphitestore.client.load(
            [f"aliasByNode(hybrid_metrics.app.{app_id}.job_group.*.test.skewness_score, 4)"],
            since="now-1h",
            until="now",
            lastrow=True,
        )

        return dbc.Col([
            dbc.Row(
                children=[
                    dbc.Card(
                        [html.H5("Containers"), html.P(containers)],
                        body=True,
                        style=common_style
                    ),
                    dbc.Card(
                        [html.H5("Jobs Running"), html.P(jobs_running)],
                        body=True,
                        style=common_style
                    ),
                ]),
            dbc.Row(
                [
                    dbc.Card([
                        html.H6(f"S.T. for {name}"),
                        html.P(f"{score:.2f}", style={"font-size": "12pt"}),
                    ], style={**common_style, "padding": "3px"})
                    for name, (score, *_) in scores[0].items() if name.startswith("job_group")
                ]
            ),
            dbc.Row(dbc.Card(
                [html.H5("Jobs run time stats"), p_data],
                body=True,
                style=wide_style
            ))
        ])

    def add_callbacks(self, app):
        @app.callback(
            Output(self.container_id, "children"), Input("selected-app-info", "data"),
            Input("interval", "n_intervals")
        )
        def update_state(selected_app_info, n_intervals):
            if not n_intervals or not n_intervals % 2 == 0:
                raise PreventUpdate
            if not selected_app_info:
                raise PreventUpdate
            app_id: Optional[ApplicationMetrics] = selected_app_info["app_id"]
            seq_job_data_raw = kvstore.client.zrevrangebyscore(
                kvinfo.sequential_jobs_key(app_id=app_id), min=0, max=500000, num=30, start=0
            )
            seq_job_data = [JobStages.from_json(x) for x in seq_job_data_raw]
            return self.render_app_info(app_id, selected_app_info.get("environment"), seq_job_data)
