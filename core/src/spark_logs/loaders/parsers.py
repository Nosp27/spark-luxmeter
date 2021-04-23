import abc

import json

from lxml import html


class BaseParser:
    resp_format = None

    def execute(self, response_data):
        return self._parse(response_data)

    def _parse(self, data):
        return data


class JsonParser(BaseParser):
    resp_format = "json"


class StageIds(JsonParser):
    def __init__(self, active_only=True):
        self.active_only = active_only

    def _check(self, row):
        return (row["status"] == "ACTIVE") if self.active_only else True

    def _parse(self, data):
        return [row["stageId"] for row in data if self._check(row)]


class JobIds(JsonParser):
    def __init__(self, *, active_only):
        self.active_only = active_only

    def _check(self, row):
        return (row["status"] == "ACTIVE") if self.active_only else True

    def _parse(self, data):
        return [
            {"jobId": row["jobId"], "stageIds": row["stageIds"]}
            for row in data
            if self._check(row)
        ]


class StageExtendedParser(JsonParser):
    def _parse(self, data):
        data, *_ = data  # Stage data is a list of one element
        tasks = {t["taskId"]: t for t in data["tasks"].values()}
        del data["tasks"]
        return data, tasks


class AppIdsFromHtml(BaseParser):
    resp_format = "html"

    def parse_headers(self, data):
        header_elements = data.xpath("//table[@id='apps']//th/text()")
        return [header.replace(" ", "").replace("\n", "") for header in header_elements]

    def parse_metrics(self, data):
        applications_data_raw = data.xpath("//table[@id='apps']/script")[
            0
        ].text_content()
        app_data_tuples = json.loads(self._process_raw_data(applications_data_raw))
        return app_data_tuples

    def _parse(self, data):
        parsed_data = html.fromstring(data)
        headers = self.parse_headers(parsed_data)
        metrics = self.parse_metrics(parsed_data)
        applications = [dict(zip(headers, m)) for m in metrics]
        return applications

    def postprocess_metrics(self, metrics):
        for m in metrics:
            m["ID"] = html.fromstring(m["ID"]).text
            del m["TrackingUI"]
            del m["Progress"]
        return metrics

    def _process_raw_data(self, raw) -> str:
        processed = raw.split("=", 1)[1]
        processed = processed.replace("\\", "\\\\")
        return processed
