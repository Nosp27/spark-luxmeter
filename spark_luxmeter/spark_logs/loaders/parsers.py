import abc

import json

import pandas as pd
from lxml import html


class BaseParser:
    @abc.abstractmethod
    def parse_headers(self, data):
        pass

    @abc.abstractmethod
    def parse_metrics(self, data):
        pass

    @abc.abstractmethod
    def read_response(self, response):
        pass

    def execute(self, response):
        data = self.read_response(response)
        headers = self.parse_headers(data)
        metrics = self.parse_metrics(data)
        df = pd.DataFrame(metrics, columns=headers)
        df = self.postprocess_metrics(df)
        return df

    def postprocess_metrics(self, df: pd.DataFrame):
        return df


class JsonParser(BaseParser):
    def read_response(self, response):
        return response.json()

    def parse_headers(self, data):
        return list(data.keys())

    def parse_metrics(self, data):
        return list(data.values())


class AppInfoJsonParser(JsonParser):
    def read_response(self, response):
        json_resp = response.json()
        json_resp["attempts"] = len(json_resp["attempts"])
        return json_resp

    def parse_headers(self, data):
        return list(data.keys())

    def parse_metrics(self, data):
        return [list(data.values())]


class AppIdsFromHtml(BaseParser):
    def parse_headers(self, data):
        header_elements = data.xpath("//table[@id='apps']//th/text()")
        return [header.replace(" ", "").replace("\n", "") for header in header_elements]

    def parse_metrics(self, data):
        applications_data_raw = data.xpath("//table[@id='apps']/script")[0].text_content()
        app_data_tuples = json.loads(self._process_raw_data(applications_data_raw))
        return app_data_tuples

    def postprocess_metrics(self, df: pd.DataFrame):
        df["ID"] = df["ID"].apply(lambda x: html.fromstring(x).text)
        df = df.drop(["TrackingUI", "Progress"], axis=1)
        df = df.astype(
            {
                "StartTime": "int64",
                "FinishTime": "int64",
                "ApplicationType": "category",
                "FinalStatus": "category",
                "State": "category",
            }
        )
        return df

    def _process_raw_data(self, raw) -> str:
        processed = raw.split("=", 1)[1]
        processed = processed.replace("\\", "\\\\")
        return processed

    def read_response(self, response):
        return html.fromstring(response.text)
