import pandas as pd
import time
import os
import random

from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, to_utc_timestamp
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from typing import Iterator

CHECKPOINT_DIR = "checkpoints/"
INPUT_DIR = "read-data/2-user-session-window-backfill-in-microbatch.csv"


def util_generate_data(duration_seconds: int = 20, records_per_batch: int = 20) -> None:
    start_ts = datetime.now(timezone.utc)
    batch = 0
    while (datetime.now(timezone.utc) - start_ts).total_seconds() <= duration_seconds:
        df = pd.DataFrame(
            {
                "user_id": [random.randint(0, 100) for _ in range(records_per_batch)],
                "event_time": [
                    datetime.now(timezone.utc) for _ in range(records_per_batch)
                ],
            }
        )
        df.set_index("user_id").to_csv(f"{INPUT_DIR}/{batch}.csv", index=True)
        time.sleep(3)
        batch += 1
    time.sleep(6)  # wait for expiration


def util_clean_file_visible():
    for filename in os.listdir(INPUT_DIR):
        os.unlink(f"{INPUT_DIR}/{filename}")


def backfill_sessions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df["event_time"] = pd.to_datetime(df["event_time"])

    df["time_diff"] = df["event_time"].diff().dt.total_seconds()
    df["session_break"] = (df["time_diff"] > 5).fillna(False).astype(int)
    df["session_id"] = df["session_break"].cumsum()
    df["is_ongoing"] = (
        df.groupby(["session_id"])["event_time"]
        .transform("max")
        .add(pd.Timedelta(seconds=5))
        .gt(pd.to_datetime("now"))
    )
    df = df[["user_id", "event_time", "session_id", "is_ongoing"]]

    expired_sessions_df = (
        df[~df["is_ongoing"]]
        .groupby("session_id")
        .agg(
            user_id=("user_id", "first"),
            first_event_time=("event_time", "min"),
            last_event_time=("event_time", "max"),
            number_events=("event_time", "count"),
        )
        .reset_index(drop=True)
    )[["user_id", "first_event_time", "last_event_time", "number_events"]]

    ongoing_sessions_df = df[df["is_ongoing"] == True][["user_id", "event_time"]]
    return expired_sessions_df, ongoing_sessions_df


def session_state_fn(
    key: tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState
) -> Iterator[pd.DataFrame]:
    """
    This session is simplified because:
    If there is a gap in between the micro-batch ingested, it will not be noticed i.e
        only gaps between batches triggered bu the timeout will be emitted
    """
    dfs = list(pdfs)
    if len(dfs) == 0:
        return
    df = (
        pd.concat(dfs, axis=0)
        .sort_values(by=["user_id", "event_time"])
        .reset_index(drop=True)
    )
    backfilled_sessions_df, ongoing_events_df = backfill_sessions(df)

    if ongoing_events_df.empty:
        if backfilled_sessions_df.empty:
            yield pd.DataFrame()
        else:
            yield backfilled_sessions_df
    else:
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
            ).sort_values(by="user_id")
        else:
            curr_last_event_time = ongoing_events_df.iloc[-1]["event_time"]
            curr_count = len(ongoing_events_df)
            if state.exists:  # New state if session already exists
                (first_event_time, _, old_count) = state.get
                (new_first_event_time, new_last_event_time, new_count) = (
                    first_event_time,  # keep first event
                    curr_last_event_time,  # take new max event
                    old_count + curr_count,  # update number of new events
                )
            else:  # Create first state
                curr_first_event_time = ongoing_events_df.iloc[0]["event_time"]
                new_first_event_time, new_last_event_time, new_count = (
                    curr_first_event_time,
                    curr_last_event_time,
                    curr_count,
                )
            state.update((new_first_event_time, new_last_event_time, new_count))
            # Set the timeout as 5 seconds.
            state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + 5_000)
            yield pd.DataFrame()


if __name__ == "__main__":
    util_clean_file_visible()
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
        StructType().add("user_id", "string").add("event_time", "timestamp")
    )

    events_df = (
        spark.readStream.option("sep", ",")
        .schema(user_data_schema)
        .csv(INPUT_DIR, header=True)
    )

    output_struct_type = "user_id STRING, first_event_time TIMESTAMP, last_event_time TIMESTAMP, number_events INTEGER"
    state_struct_type = (
        "first_event_time TIMESTAMP, last_event_time TIMESTAMP, number_events INTEGER"
    )

    session_df = (
        events_df.withWatermark("event_time", "1 seconds")
        .groupBy(col("user_id"))
        .applyInPandasWithState(
            session_state_fn,  # type: ignore
            output_struct_type,
            state_struct_type,
            "append",
            GroupStateTimeout.EventTimeTimeout,
        )
    )

    query = (
        session_df.writeStream.outputMode("append")
        .foreachBatch(lambda df, _: df.orderBy("user_id").show(truncate=False))
        .start()
    )

    while not query.isActive:
        print(f"waiting...")
        time.sleep(1)

    util_generate_data(20)
    query.stop()
    util_clean_file_visible()
