import random
from datetime import datetime, timedelta

import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go

from dash.dependencies import Output, Input

from frontend.components.abc import Component


class MemoryPlot(Component):
    def __init__(self):
        self.figure = None

    def generate_sample_data(self, size=100):
        step = 3100
        deltas = [datetime.now()]
        for _ in range(size):
            deltas.append(deltas[-1] + timedelta(milliseconds=step))
            step += random.randrange(20, 5000)
        sample_data = {
            "Shuffle": [random.randrange(1, 10) for _ in range(size)],
            "GC": [random.randrange(1, 10) for _ in range(size)],
        }
        sample_data["Free"] = [
            25 - s - g for s, g in zip(sample_data["Shuffle"], sample_data["GC"])
        ]
        return deltas, sample_data

    def random_plot(self):
        return self.compose_layout(*self.generate_sample_data())

    def random_figure(self):
        return self.compose_figure(*self.generate_sample_data())

    def compose_layout(self, timestamps, values):
        self.figure = self.compose_figure(timestamps, values)
        return html.Div(children=dcc.Graph(id="mem-plot", figure=self.figure))

    def compose_figure(self, timestamps, values):
        delays = [
            (timestamps[i + 1] - timestamps[i]).total_seconds() * 1000
            for i in range(len(timestamps) - 1)
        ]
        fig = go.Figure()
        colors = {
            "Free": "lightgray",
            "GC": "rosybrown",
            "Shuffle": "skyblue",
        }
        for key, value in values.items():
            fig.add_bar(
                x=timestamps, y=value, name=key, width=delays, marker_color=colors[key]
            )
        fig.update_layout(
            barmode="relative",
            title_text="Memory consumption",
            transition_duration=500,
        )
        return fig
