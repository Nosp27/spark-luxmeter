import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash import dash
from frontend.components import MemoryPlot, TaskSummary, TaskList

external_stylesheets = [dbc.themes.BOOTSTRAP, "https://codepen.io/chriddyp/pen/bWLwgP.css"]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

theme = {
    "dark": True,
    "detail": "#007439",
    "primary": "#00EA64",
    "secondary": "#6E6E6E",
}


components = dict(
    mem_plot=MemoryPlot(),
    task_selector=TaskList(),
    tasks_summary=TaskSummary(),
)


if __name__ == "__main__":
    app.layout = html.Div(id="components", children=[
        html.Div(
            children=[
                html.Button("Regenerate", "regenerate-btn", style=dict(display="block")),
                components["task_selector"].compose_layout(components["task_selector"].generate_sample_data()),
                components["mem_plot"].random_plot(),
                components["tasks_summary"].plot(),
                dcc.Store(id="store", data=dict()),
            ]
        )
    ])

    for component in components.values():
        component.add_callbacks(app)

    app.run_server(debug=True)
