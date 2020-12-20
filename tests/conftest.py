import json
import random
import uuid

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from threading import Thread
import pytest
import os

from pyspark.sql.types import StructType, ArrayType


@pytest.fixture(autouse=True)
@pytest.mark.asyncio
def pytestmark():
    yield


@pytest.fixture(scope="session")
def create_temp_spark_data():
    try:
        os.mkdir("./test_spark_data/")
        os.mkdir("./test_spark_data/checkpoints")
        os.mkdir("./test_spark_data/in")
        os.mkdir("./test_spark_data/out")

        for filename in range(3):
            with open(f"./test_spark_data/{filename}.json", "w+") as file:
                data = [
                    dict(
                        zip(
                            ["id", "group_name", "score"],
                            [
                                uuid.uuid4().hex,
                                random.choice(
                                    ["alpha", "beta", "omega"], random.randint(10, 100)
                                ),
                            ],
                        )
                    )
                    for _ in range(100)
                ]

                file.write(json.dumps(data))
        yield
    finally:
        os.system("rm -rf ./test_spark_data")


@pytest.fixture
def spark_session():
    spark = SparkSession.builder.appName("testSparkSession").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def spark_execute_tasks(spark_session):
    schema = (
        StructType()
        .add("id", "string")
        .add("name", "string")
        .add("attempts", ArrayType(StructType().add("duration", "integer")))
    )

    input_data = spark_session.readStream.json(
        path="./test_spark_data/in/", schema=schema
    )

    processing = (
        input_data.select(F.explode("attempts"))
        .writeStream.outputMode("Append")
        .format("json")
        .option("path", "./test_spark_data/out/")
        .option("checkpointLocation", "./test_spark_data/checkpoints")
        .start()
    )

    Thread(target=lambda: processing.awaitTermination()).start()
    yield
    processing.stop()
