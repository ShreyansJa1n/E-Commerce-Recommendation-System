"""Build a small, deterministic raw sample (same CSV format as the real data).

Visitors are selected by hashing ``visitorid`` with a salt, so the sample is stable
across runs. Item properties are kept for every item the sampled visitors touched;
the category tree is copied in full.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from recsys import schemas
from recsys.config import Config, SampleConfig
from recsys.io import StageReport, timed_stage

_BUCKETS = 10_000


def sample_visitors(events: DataFrame, cfg: SampleConfig) -> DataFrame:
    bucket = F.pmod(F.xxhash64(F.lit(cfg.salt), F.col("visitorid")), F.lit(_BUCKETS))
    return events.where(bucket < F.lit(int(cfg.visitor_fraction * _BUCKETS)))


def _write_single_csv(df: DataFrame, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=out.parent) as tmp:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp + "/csv")
        (part,) = Path(tmp, "csv").glob("part-*.csv")
        shutil.move(str(part), out)


def run(spark: SparkSession, source: Config, target: Config) -> StageReport:
    src, dst = source.paths.resolved().raw, target.paths.resolved().raw
    if src == dst:
        raise ValueError("Sample source and target raw paths are the same")
    ing = source.ingest

    def read(files: list[str], schema: StructType) -> DataFrame:
        return (
            spark.read.option("header", "true")
            .option("enforceSchema", "false")
            .schema(schema)
            .csv([str(src / f) for f in files])
        )

    with timed_stage("sample") as report:
        events = sample_visitors(read(ing.events_files, schemas.RAW_EVENTS), source.sample)
        events = events.orderBy("timestamp", "visitorid").cache()
        items = events.select("itemid").distinct()
        # A USING join moves the key column first; restore the raw column order, since
        # the CSV is read back positionally against the raw schema.
        props = (
            read(ing.item_properties_files, schemas.RAW_ITEM_PROPERTIES)
            .join(F.broadcast(items), "itemid", "left_semi")
            .select(*schemas.RAW_ITEM_PROPERTIES.fieldNames())
        )
        tree = read([ing.category_tree_file], schemas.RAW_CATEGORY_TREE)

        _write_single_csv(events, dst / ing.events_files[0])
        # Keep the multi-part layout so the sample exercises the same ingest code path.
        props = props.cache()
        prop_parts = ing.item_properties_files
        for i, name in enumerate(prop_parts):
            part = props.where(F.pmod(F.xxhash64("itemid"), F.lit(len(prop_parts))) == i)
            _write_single_csv(part.orderBy("timestamp", "itemid", "property"), dst / name)
        _write_single_csv(tree, dst / ing.category_tree_file)

        report.rows = {
            "events": events.count(),
            "item_properties": props.count(),
            "category_tree": tree.count(),
        }
        report.extra = {"visitor_fraction": source.sample.visitor_fraction, "target": str(dst)}
    report.write(dst / "_reports")
    return report
