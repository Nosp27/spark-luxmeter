import os
import pathlib

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_table
import plotly.graph_objs as go
import dash_daq as daq
import requests

import pandas as pd

app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server
app.config["suppress_callback_exceptions"] = True

APP_PATH = str(pathlib.Path(__file__).parent.resolve())


suffix_row = "_row"
suffix_button_id = "_button"
suffix_sparkline_graph = "_sparkline_graph"
suffix_count = "_count"
suffix_ooc_n = "_OOC_number"
suffix_ooc_g = "_OOC_graph"
suffix_indicator = "_indicator"


def build_storage():
    return dcc.Store(id="store", data=dict())


def build_banner():
    return html.Div(
        id="banner",
        className="banner",
        children=[
            html.Div(
                id="banner-text",
                children=[
                    html.H5("Spark Application Monitoring"),
                    html.H6("Anomaly reporting and hybrid metrics system"),
                ],
            ),
        ],
    )


def build_tabs():
    return html.Div(
        id="tabs",
        className="tabs",
        children=[
            dcc.Tabs(
                id="app-tabs",
                value="tab2",
                className="custom-tabs",
                children=[
                    # dcc.Tab(
                    #     id="Settings-tab",
                    #     label="Settings",
                    #     value="tab1",
                    #     className="custom-tab",
                    #     selected_className="custom-tab--selected",
                    # ),
                    dcc.Tab(
                        id="Control-chart-tab",
                        label="Dashboard",
                        value="tab2",
                        className="custom-tab",
                        selected_className="custom-tab--selected",
                    ),
                ],
            )
        ],
    )


def build_tab_1():
    return [
        # Manually select metrics
        html.Div(
            id="set-specs-intro-container",
            # className='twelve columns',
            children=html.P(
                "Use historical control limits to establish a benchmark, or set new values."
            ),
        ),
        html.Div(
            id="settings-menu",
            children=[
                html.Div(
                    id="value-setter-menu",
                    # className='six columns',
                    children=[
                        html.Div(id="value-setter-panel"),
                        html.Br(),
                        html.Div(
                            id="button-div",
                            children=[
                                html.Button("Update", id="value-setter-set-btn"),
                                html.Button(
                                    "View current setup",
                                    id="value-setter-view-btn",
                                    n_clicks=0,
                                ),
                            ],
                        ),
                        html.Div(
                            id="value-setter-view-output", className="output-datatable"
                        ),
                    ],
                ),
            ],
        ),
    ]


def build_quick_stats_panel():
    return html.Div(
        id="quick-stats",
        className="row",
        children=[
            html.Div(
                id="card-1",
                children=[
                    html.P("Stages count"),
                    daq.Gauge(
                        id="stage-count",
                        max=100,
                        min=0,
                        showCurrentValue=True,  # default size 200 pixel
                    ),
                ],
            ),
            html.Div(
                id="card-2",
                children=[
                    html.P("Time to completion"),
                    daq.Gauge(
                        id="progress-gauge",
                        max=100,
                        min=0,
                        showCurrentValue=True,  # default size 200 pixel
                    ),
                ],
            ),
        ],
    )


def generate_section_banner(title):
    return html.Div(className="section-banner", children=title)


def build_top_panel(metrics_data):
    return html.Div(
        id="top-section-container",
        className="row",
        children=[
            # Metrics summary
            html.Div(
                id="metric-summary-session",
                className="eight columns",
                children=[
                    generate_section_banner("Process Control Metrics Summary"),
                    html.Div(
                        id="metric-div",
                        children=[
                            generate_metric_list_header(),
                            html.Div(
                                id="metric-rows",
                                children=[
                                    generate_metric_row_helper(
                                        [x[1] for x in data["datapoints"]],
                                        [x[0] for x in data["datapoints"]],
                                        f"metric-{idx}"
                                    )
                                    for idx, data in enumerate(metrics_data)
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )


# Build header
def generate_metric_list_header():
    return generate_metric_row(
        "metric_header",
        {"height": "3rem", "margin": "1rem 0", "textAlign": "center"},
        {"id": "m_header_1", "children": html.Div("Metric")},
        {"id": "m_header_2", "children": html.Div("Count")},
        {"id": "m_header_3", "children": html.Div("")},
        {"id": "m_header_4", "children": html.Div("Score%")},
        {"id": "m_header_5", "children": html.Div("%Score")},
        {"id": "m_header_6", "children": ""},
    )


def generate_metric_row_helper(x_data, y_data, metric_name):
    div_id = metric_name + suffix_row
    button_id = metric_name + suffix_button_id
    sparkline_graph_id = metric_name + suffix_sparkline_graph
    count_id = metric_name + suffix_count
    ooc_percentage_id = metric_name + suffix_ooc_n
    ooc_graph_id = metric_name + suffix_ooc_g
    indicator_id = metric_name + suffix_indicator

    return generate_metric_row(
        div_id,
        None,
        {
            "id": metric_name,
            "className": "metric-row-button-text",
            "children": html.Button(
                id=button_id,
                className="metric-row-button",
                children=metric_name,
                title="Click to visualize live SPC chart",
                n_clicks=0,
            ),
        },
        {"id": count_id, "children": "0"},
        {
            "id": metric_name + "_sparkline",
            "children": dcc.Graph(
                id=sparkline_graph_id,
                style={"width": "100%", "height": "95%"},
                config={
                    "staticPlot": False,
                    "editable": False,
                    "displayModeBar": False,
                },
                figure=go.Figure(
                    {
                        "data": [
                            {
                                "x": x_data,
                                "y": y_data,
                                "mode": "lines+markers",
                                "name": metric_name,
                                "line": {"color": "#f4d44d"},
                            }
                        ],
                        "layout": {
                            "uirevision": True,
                            "margin": dict(l=0, r=0, t=4, b=4, pad=0),
                            "xaxis": dict(
                                showline=False,
                                showgrid=False,
                                zeroline=False,
                                showticklabels=False,
                            ),
                            "yaxis": dict(
                                showline=False,
                                showgrid=False,
                                zeroline=False,
                                showticklabels=False,
                            ),
                            "paper_bgcolor": "rgba(0,0,0,0)",
                            "plot_bgcolor": "rgba(0,0,0,0)",
                        },
                    }
                ),
            ),
        },
        {"id": ooc_percentage_id, "children": "0.00%"},
        {
            "id": ooc_graph_id + "_container",
            "children": daq.GraduatedBar(
                id=ooc_graph_id,
                color={
                    "ranges": {
                        "#92e0d3": [0, 3],
                        "#f4d44d ": [3, 7],
                        "#f45060": [7, 15],
                    }
                },
                showCurrentValue=False,
                max=15,
                value=0,
            ),
        },
        {
            "id": metric_name + "_pf",
            "children": daq.Indicator(
                id=indicator_id, value=True, color="#91dfd2", size=12
            ),
        },
    )


def generate_metric_row(id, style, *cols):
    if style is None:
        style = {"height": "8rem", "width": "100%"}

    return html.Div(
        id=id,
        className="row metric-row",
        style=style,
        children=[
            html.Div(
                id=col["id"],
                className="one column",
                style={"margin-right": "2.5rem", "minWidth": "50px"},
                children=col["children"],
            )
            for col in cols
        ],
    )


def update_count(data):
    size = len(data["datapoints"])
    return str(size), "#91dfd2"


app.layout = html.Div(
    id="big-app-container",
    children=[
        build_storage(),
        build_banner(),
        dcc.Interval(
            id="interval-component",
            interval=2 * 1000,  # in milliseconds
            n_intervals=50,  # start at batch 50
        ),
        html.Div(
            id="app-container",
            children=[
                build_tabs(),
                # Main app
                html.Div(id="app-content"),
            ],
        ),
        dcc.Store(id="n-interval-stage", data=50),
    ],
)


@app.callback(
    [Output("app-content", "children")],
    [Input("app-tabs", "value")],
    [State("store", "data")],
)
def render_tab_content(tab_switch, store):
    if tab_switch == "tab1":
        return build_tab_1(),
    return html.Div(
        id="status-container",
        children=[
            build_quick_stats_panel(),
            html.Div(
                id="graphs-container",
                children=[build_top_panel(store)],
            ),
        ],
    ),


# Update interval
@app.callback(
    Output("n-interval-stage", "data"),
    [Input("app-tabs", "value")],
    [
        State("interval-component", "n_intervals"),
        State("interval-component", "disabled"),
        State("n-interval-stage", "data"),
    ],
)
def update_interval_state(tab_switch, cur_interval, disabled, cur_stage):
    if disabled:
        return cur_interval

    if tab_switch == "tab1":
        return cur_interval
    return cur_stage


# ======= update progress gauge =========
@app.callback(
    output=[Output("progress-gauge", "value"), Output("stage-count", "value")],
    inputs=[Input("interval-component", "n_intervals")],
)
def update_gauge(interval):
    import random

    return 78 + random.randint(0, 10), int(interval)


@app.callback(
    output=Output("value-setter-view-output", "children"),
    inputs=[
        Input("value-setter-view-btn", "n_clicks"),
        Input("metric-select-dropdown", "value"),
        Input("value-setter-store", "data"),
    ],
)
def show_current_specs(n_clicks, dd_select, store_data):
    if n_clicks > 0:
        curr_col_data = store_data[dd_select]
        new_df_dict = {
            "Specs": [
                "Upper Specification Limit",
                "Lower Specification Limit",
                "Upper Control Limit",
                "Lower Control Limit",
            ],
            "Current Setup": [
                curr_col_data["usl"],
                curr_col_data["lsl"],
                curr_col_data["ucl"],
                curr_col_data["lcl"],
            ],
        }
        new_df = pd.DataFrame.from_dict(new_df_dict)
        return dash_table.DataTable(
            style_header={"fontWeight": "bold", "color": "inherit"},
            style_as_list_view=True,
            fill_width=True,
            style_cell_conditional=[
                {"if": {"column_id": "Specs"}, "textAlign": "left"}
            ],
            style_cell={
                "backgroundColor": "#1e2130",
                "fontFamily": "Open Sans",
                "padding": "0 2rem",
                "color": "darkgray",
                "border": "none",
            },
            css=[
                {"selector": "tr:hover td", "rule": "color: #91dfd2 !important;"},
                {"selector": "td", "rule": "border: none !important;"},
                {
                    "selector": ".dash-cell.focused",
                    "rule": "background-color: #1e2130 !important;",
                },
                {"selector": "table", "rule": "--accent: #1e2130;"},
                {"selector": "tr", "rule": "background-color: transparent"},
            ],
            data=new_df.to_dict("rows"),
            columns=[{"id": c, "name": c} for c in ["Specs", "Current Setup"]],
        )


# decorator for list of output
def create_callback(param):
    def callback(stored_data):
        count, indicator = update_count(
            stored_data
        )
        return count, spark_line_data, indicator

    return callback


tracked_metrics = {
    "anomaly_detection.metrics_sample",
}


for param in tracked_metrics:
    update_param_row_function = create_callback(param)
    app.callback(
        output=[
            Output(param + suffix_count, "children"),
            Output(param + suffix_sparkline_graph, "extendData"),
            Output(param + suffix_ooc_n, "children"),
            Output(param + suffix_ooc_g, "value"),
            Output(param + suffix_indicator, "color"),
        ],
        inputs=[Input("interval-component", "n_intervals")],
        state=[State("store", "data")],
    )(update_param_row_function)


def request_graphite_data(metrics):
    endpoint = os.environ["METRICS_ENDPOINT"].split(":")
    resp = requests.get(
        "http://" + endpoint + "/render", params={"target": metrics, "format": "json"}
    )
    resp.raise_for_status()
    data = resp.json()
    notnull_data = {
        metric["target"]: [
            point for point in metric["datapoints"] if point[0] is not None
        ]
        for metric in data
    }
    return notnull_data


# Running the server
if __name__ == "__main__":
    app.run_server(debug=True, port=8050)
