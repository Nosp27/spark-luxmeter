import requests
import os

from spark_logs.loaders import fetchers
from unittest.mock import patch


def test_app_list_fetcher():
    with patch.object(requests, "get") as get_mock:
        get_mock.return_value.text.return_value = open(
            os.path.join("../..", "share", "sample_applications_page.html")
        ).read()
        fetcher = fetchers.HttpFetcher(config={"base_url": "http://somesite.com"})
        fetcher.fetch(node="applications", resp_format="html")
