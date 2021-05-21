from datetime import datetime

import requests

from spark_logs.time_series_tools import interpolate


class GraphiteReader:
    def __init__(self, graphite_server, graphite_port):
        self.graphite_server = graphite_server
        self.graphite_port = graphite_port

    def load(self, keys, since=None, until=None, interpolation=False):
        params = [("format", "json")]
        if since and until:
            params.extend([("from", since), ("until", until)])
        params.extend([("target", key) for key in keys])

        resp = requests.get(
            f"http://{self.graphite_server}:{self.graphite_port}/render", params=params,
        )
        resp.raise_for_status()

        result_json = resp.json()
        result = {metric["target"]: metric["datapoints"] for metric in result_json}
        if interpolation:
            result = {k: interpolate(v) for k, v in result.items()}

        timestamps = [datetime.fromtimestamp(x[1]) for x in next(iter(result.values()))]
        result = {
            key: [x[0] for x in metric_zipped] for key, metric_zipped in result.items()
        }
        return result, timestamps


client = GraphiteReader(graphite_server="localhost", graphite_port=55010)
