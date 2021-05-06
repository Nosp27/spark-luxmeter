import abc

import json

from lxml import html

from spark_logs.types import Stage, Job, Task, StageTasks


class BaseParser:
    resp_format = None
    node_cls = None

    def execute(self, response_data):
        return self._node_transform(self._parse(response_data))

    def _parse(self, data):
        return data

    def _node_transform(self, data):
        if self.node_cls is None:
            return data
        d = [self.node_cls.create_from_dict(x) for x in data]
        return d


class JsonParser(BaseParser):
    resp_format = "json"


class StageIds(JsonParser):
    def __init__(self, active_only=True):
        self.active_only = active_only

    def _check(self, row):
        return (row["status"] == "ACTIVE") if self.active_only else True

    def _parse(self, data):
        return [row["stageId"] for row in data if self._check(row)]


class Jobs(JsonParser):
    node_cls = Job

    def __init__(self, *, inactive_only):
        self.inactive_only = inactive_only

    def _check(self, row):
        return (row["status"] not in ("ACTIVE",)) if self.inactive_only else True

    def _parse(self, data):
        return [row for row in data if self._check(row)]


class StageExtendedParser(JsonParser):
    node_cls = StageTasks
    stage_node_cls = Stage
    task_node_cls = Task

    def _parse(self, data):
        data, *_ = data  # Stage data is a list of one element
        tasks = {str(t["taskId"]): t for t in data["tasks"].values()}
        del data["tasks"]
        return data, tasks

    def _node_transform(self, data_and_tasks):
        stage, tasks = data_and_tasks
        return self.node_cls(
            stage=self.stage_node_cls.create_from_dict(stage),
            tasks={k: self.task_node_cls.create_from_dict(v) for k, v in tasks.items()},
        )


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
        return self.postprocess_metrics(applications)

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
