from datetime import datetime

import requests

from dateutil.parser import parse
from spark_logs.config import DEFAULT_CONFIG
from spark_logs.time_series_tools import interpolate

config = DEFAULT_CONFIG


class GraphiteReader:
    def __init__(self, graphite_server, graphite_reader_port):
        self.graphite_server = graphite_server
        self.graphite_reader_port = graphite_reader_port

    def load(self, keys, since=None, until=None, interpolation=False, lastrow=False, consolidation=None):
        params = [("format", "json")]
        if since and until:
            if "now" not in since:
                since = parse(since)
                since = since.strftime("%H:%M_%Y%m%d")
            if "now" not in until:
                until = parse(until)
                until = until.strftime("%H:%M_%Y%m%d")
            params.extend([("from", since), ("until", until)])
        params.extend([("target", key) for key in keys])
        if lastrow:
            params.append(("maxDataPoints", 1))
            params.append(("consolidateBy", consolidation or "last"))
        # params.append(("maxDataPoints", DEFAULT_CONFIG["graphite_maxDataPoints"]))

        resp = requests.get(
            f"http://{self.graphite_server}:{self.graphite_reader_port}/render",
            params=params,
        )
        resp.raise_for_status()

        result_json = resp.json()
        result_raw = {metric["target"]: metric["datapoints"] for metric in result_json}
        if interpolation:
            result = {k: interpolate(v) for k, v in result_raw.items()}
        else:
            result = {
                k: list(map(lambda x: 0 if x is None else x, v))
                for k, v in result_raw.items()
            }

        timestamps = [
            datetime.fromtimestamp(x[1]) for x in next(iter(result_raw.values()), [])
        ]
        try:
            result = {
                key: [x[0] for x in metric_zipped]
                for key, metric_zipped in result.items()
            }
        except TypeError as exc:
            raise
        return result, timestamps


client = GraphiteReader(
    graphite_server=config["graphite_host"],
    graphite_reader_port=config["graphite_reader_port"],
)
