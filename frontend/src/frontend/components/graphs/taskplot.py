from collections import defaultdict

from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from frontend import kvstore
from spark_logs import kvstore as kvinfo
from frontend.components import Component
import dash_core_components as dcc
from plotly import graph_objects as go

from spark_logs.types import JobStages


class TaskPlot(Component):
    def render(self):
        return dcc.Graph(figure=go.Figure(), id="taskplot-graph")

    def render_executor_task_stats(self, app_id):
        data_raw = kvstore.client.zrevrangebyscore(
            kvinfo.sequential_jobs_key(app_id=app_id), min=0, max=9999999, start=0, num=30
        )
        last_jobs = [JobStages.from_json(x) for x in data_raw]

        executor_times = defaultdict(lambda: defaultdict(lambda: 0))

        for job_stages in last_jobs:
            stages = job_stages.stages
            for stage_id, stage in stages.items():
                tasks = stage.tasks
                for _, task in tasks.items():
                    if "driver" in task.executorId:
                        continue
                    key = int(task.executorId)
                    try:
                        executors_des_time = task.taskMetrics["executorDeserializeTime"] / 10 ** 6
                        executors_des_cpu_time = task.taskMetrics["executorDeserializeCpuTime"] / 10 ** 9
                        executors_run_time = task.taskMetrics["executorRunTime"] / 10 ** 6
                        executors_cpu_time = task.taskMetrics["executorCpuTime"] / 10 ** 9
                        java_gc = task.taskMetrics["jvmGcTime"] / 10 ** 6

                        executor_times["executors_des_cpu_time"][key] += executors_des_cpu_time
                        executor_times["executors_des_nocpu_time"][key] += executors_des_time - executors_des_cpu_time
                        executor_times["java_gc"][key] += java_gc
                        executor_times["executors_cpu_time"][key] += executors_cpu_time - executors_des_cpu_time - java_gc
                        executor_times["executors_run_time"][key] += executors_run_time - executors_cpu_time - executors_des_time + executors_des_cpu_time
                    except KeyError as exc:
                        continue

        colors = {
            "executors_run_time": "lightgray",
            "executors_cpu_time": "rosybrown",
            "executors_des_nocpu_time": "yellow",
            "executors_des_cpu_time": "orange",
            "java_gc": "lightblue",
        }

        fig = go.Figure()
        for metric_name, data in executor_times.items():
            fig.add_bar(x=[*data.keys()], y=[*data.values()], name=metric_name, marker_color=colors[metric_name])
        fig.update_layout(
            barmode="relative",
            title_text="Memory distribution",
            transition_duration=500,
        )
        fig.update_yaxes(type="log")
        return fig

    def add_callbacks(self, app):
        @app.callback(
            Output("taskplot-graph", "figure"),
            Input("selected-app-info", "data"),
            Input("interval", "n_intervals"),
        )
        def update_graph(selected_app_info, n):
            if not n or not n % 4 == 0:
                raise PreventUpdate
            if not selected_app_info:
                raise PreventUpdate
            app_id = selected_app_info["app_id"]
            return self.render_executor_task_stats(app_id)
