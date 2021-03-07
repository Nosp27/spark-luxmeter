import abc

import json

from lxml import html


class BaseParser:
    @abc.abstractmethod
    def read_response(self, response):
        pass

    def execute(self, response, index):
        data = self.read_response(response)
        parsed_metrics = self.parse(data, index)
        return self.postprocess_metrics(parsed_metrics, index)

    def parse(self, data, index):
        return data

    @abc.abstractmethod
    def postprocess_metrics(self, metrics, index):
        if not isinstance(metrics, list):
            return metrics
        return {el[index]: el for el in metrics}


class JsonParser(BaseParser):
    def read_response(self, response):
        return response.json()


class AppIdsFromHtml(BaseParser):
    def parse_headers(self, data):
        header_elements = data.xpath("//table[@id='apps']//th/text()")
        return [header.replace(" ", "").replace("\n", "") for header in header_elements]

    def parse_metrics(self, data):
        applications_data_raw = data.xpath("//table[@id='apps']/script")[0].text_content()
        app_data_tuples = json.loads(self._process_raw_data(applications_data_raw))
        return app_data_tuples

    def parse(self, data, index):
        headers = self.parse_headers(data)
        metrics = self.parse_metrics(data)
        applications = [dict(zip(headers, m)) for m in metrics]
        return applications

    def postprocess_metrics(self, metrics, index):
        for m in metrics:
            m["ID"] = html.fromstring(m["ID"]).text
            del m["TrackingUI"]
            del m["Progress"]
        return super().postprocess_metrics(metrics, index)

    def _process_raw_data(self, raw) -> str:
        processed = raw.split("=", 1)[1]
        processed = processed.replace("\\", "\\\\")
        return processed

    def read_response(self, response):
        return html.fromstring(response.text)
