import pandas as pd
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from typing import Iterator

CHECKPOINT_DIR = "checkpoints/"


def session_state_fn(
    key: tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    """
    This session is simplified because:
    If there is a gap in between the micro-batch ingested, it will not get observed.
    Only gaps between batches triggered bu the timeout will be emitted
    """
    pdf: pd.DataFrame = pd.concat(list(pdfs), axis=0).sort_values(by="event_time")
    if state.hasTimedOut:
        (user_id,) = key
        (first_event_time, last_event_time, count) = state.get
        state.remove()
        yield pd.DataFrame(
            {
                "user_id": [user_id],
                "first_event_time": [first_event_time],
                "last_event_time": [last_event_time],
                "number_events": [count],
            }
        )
    else:
        curr_last_event_time = pdf.iloc[-1]["event_time"]
        curr_count = len(pdf)
        if state.exists:  # New state if session already exists
            (first_event_time, _, old_count) = state.get
            (new_first_event_time, new_last_event_time, new_count) = (
                first_event_time,  # keep first event
                curr_last_event_time,  # take new max event
                old_count + curr_count,  # update number of new events
            )
        else:  # Create first state
            curr_first_event_time = pdf.iloc[0]["event_time"]
            new_first_event_time, new_last_event_time, new_count = (
                curr_first_event_time,
                curr_last_event_time,
                curr_count,
            )
        state.update((new_first_event_time, new_last_event_time, new_count))
        # Set the timeout as 10 seconds.
        state.setTimeoutDuration(5_000)
        yield pd.DataFrame()


if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.master("local")  # type: ignore
        .appName("Python Spark Time Based Session Usecase")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    user_data_schema = (
        StructType().add("user_id", "string").add("event_time", "timestamp")
    )

    events_df = (
        spark.readStream.option("sep", ",")
        .schema(user_data_schema)
        .csv("data/1-user-session-window-data.csv", header=True)
    ).withWatermark("event_time", "5 seconds")

    output_struct_type = "user_id STRING, first_event_time TIMESTAMP, last_event_time TIMESTAMP, number_events INTEGER"
    state_struct_type = (
        "first_event_time TIMESTAMP, last_event_time TIMESTAMP, number_events INTEGER"
    )

    session_df = events_df.groupBy(col("user_id")).applyInPandasWithState(
        session_state_fn,  # type: ignore
        output_struct_type,
        state_struct_type,
        "append",
        GroupStateTimeout.ProcessingTimeTimeout,
    )

    query = session_df.writeStream.format("console").outputMode("append").start()

    time.sleep(20.0)
    query.stop()
