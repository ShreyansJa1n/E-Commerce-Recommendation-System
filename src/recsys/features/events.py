"""Silver events -> ``gold/events_enriched``: session ids and point-in-time category.

Both additions depend only on each event's own past: a session boundary is decided by
the gap to the *previous* event, and the category is the catalog value valid at the
event's own timestamp. Appending later events never changes earlier rows.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys.clean.catalog import property_as_of


def sessionize(events: DataFrame, gap_minutes: int) -> DataFrame:
    """Add ``session_id`` (``"<visitor_id>:<n>"``): a new session starts after a gap."""
    by_visitor = Window.partitionBy("visitor_id").orderBy("ts_ms", "event_type", "item_id")
    gap_ms = gap_minutes * 60_000
    prev = F.lag("ts_ms").over(by_visitor)
    starts = events.withColumn(
        "_new_session", (prev.isNull() | (F.col("ts_ms") - prev > gap_ms)).cast("int")
    )
    running = by_visitor.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return starts.withColumn(
        "session_id",
        F.concat_ws(
            ":",
            F.col("visitor_id").cast("string"),
            F.sum("_new_session").over(running).cast("string"),
        ),
    ).drop("_new_session")


def enrich_events(events: DataFrame, scd: DataFrame, gap_minutes: int) -> DataFrame:
    with_cat = property_as_of(events, scd, "categoryid", "event_ts", "_category")
    return (
        sessionize(with_cat, gap_minutes)
        .withColumn("category_id", F.col("_category").try_cast("int"))
        .select(
            "event_ts",
            "ts_ms",
            "visitor_id",
            "item_id",
            "event_type",
            "transaction_id",
            "session_id",
            "category_id",
            "event_date",
        )
    )
