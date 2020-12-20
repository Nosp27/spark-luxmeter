import asyncio

import pytest

from spark_luxmeter.spark_logs.metrics import MetricsClient


@pytest.mark.asyncio
async def test_get_local_spark_metrics(spark_session):
    metrics_client = MetricsClient("localhost")
    applications = await metrics_client.get_spark_metrics("applications")
    assert isinstance(applications, list)
    json_first, *_ = applications
    assert "id" in json_first
    assert "name" in json_first
