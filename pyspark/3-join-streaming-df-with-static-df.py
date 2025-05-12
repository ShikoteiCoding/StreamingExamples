import pandas as pd
import time
import os
import random

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

CHECKPOINT_DIR = "checkpoints/"
INPUT_STREAMING_DIR = "read-data/3-join-streaming-df-with-static-df-streaming.csv"
INPUT_STATIC_DIR = "read-data/3-join-streaming-df-with-static-df-static.csv"

def util_generate_static(join_key_cardinality: int = 5):
    df = pd.DataFrame(
        {
            "id": [i for i in range(join_key_cardinality)],
            "size": [random.randint(100, 1_000) for _ in range(join_key_cardinality)]
        }
    )
    df.set_index("id").to_csv(f"{INPUT_STATIC_DIR}/0.csv", index=True)

def util_generate_data(duration_seconds: int = 20, records_per_batch: int = 20, join_key_cardinality: int = 5) -> None:
    start_ts = datetime.now(timezone.utc)
    batch = 0
    while (datetime.now(timezone.utc) - start_ts).total_seconds() <= duration_seconds:
        df = pd.DataFrame(
            {
                "user_id": [random.randint(0, 100) for _ in range(records_per_batch)],
                "event_time": [
                    datetime.now(timezone.utc) for _ in range(records_per_batch)
                ],
                "company_id": [random.randint(0, join_key_cardinality) for _ in range(records_per_batch)]
            }
        )
        df.set_index("user_id").to_csv(f"{INPUT_STREAMING_DIR}/{batch}.csv", index=True)
        time.sleep(3)
        batch += 1
    time.sleep(6)  # wait for expiration


def util_clean_file_visible():
    for filename in os.listdir(INPUT_STREAMING_DIR):
        os.unlink(f"{INPUT_STREAMING_DIR}/{filename}")

    for filename in os.listdir(INPUT_STATIC_DIR):
        os.unlink(f"{INPUT_STATIC_DIR}/{filename}")


if __name__ == "__main__":
    util_clean_file_visible()
    util_generate_static()
    spark: SparkSession = (
        SparkSession.builder.master("local")  # type: ignore
        .appName("Python Spark Time Based Session Usecase")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    user_data_schema = (
        StructType().add("user_id", "string").add("event_time", "timestamp").add("company_id", "string")
    )
    user_df = (
        spark.readStream.option("sep", ",")
        .option("maxFilesPerTrigger", "1")
        .schema(user_data_schema)
        .csv(INPUT_STREAMING_DIR, header=True)
    )

    company_data_schema = (
        StructType().add("id", "string").add("size", "integer")
    )
    company_df = (
        spark.read.option("sep", ",")
        .schema(company_data_schema)
        .csv(INPUT_STATIC_DIR, header=True)
    )

    results_df = user_df.join(company_df, user_df.company_id == company_df.id, how="inner")

    query = (
        results_df.writeStream.outputMode("append")
        .foreachBatch(lambda df, _: df.orderBy("user_id").show(truncate=False))
        .start()
    )

    while not query.isActive:
        print(f"waiting...")
        time.sleep(1)

    util_generate_data(20)
    query.stop()
