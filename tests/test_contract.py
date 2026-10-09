import pytest
from pyspark.sql import SparkSession

from recsys.config import REPO_ROOT, load_config
from recsys.features import contract


def test_feature_contract_doc_is_up_to_date() -> None:
    doc = REPO_ROOT / "docs" / "feature_contract.md"
    assert doc.read_text() == contract.render_markdown(load_config("base")), (
        "docs/feature_contract.md is stale: run `make contract`"
    )


def test_specs_have_unique_names() -> None:
    for name, specs in contract.table_specs(load_config("base").features).items():
        names = [s.name for s in specs]
        assert len(names) == len(set(names)), name


def test_conform_rejects_drift(spark: SparkSession) -> None:
    specs = [
        contract.Spec("a", "int", "-", contract.NEVER, ""),
        contract.Spec("b", "double", "-", contract.NEVER, ""),
    ]
    ok = spark.createDataFrame([(1.0, 1)], "b double, a int")
    assert contract.conform(ok, specs, "t").columns == ["a", "b"]
    with pytest.raises(contract.ContractError, match="b: bigint != double"):
        contract.conform(spark.createDataFrame([(1, 1)], "a int, b bigint"), specs, "t")
    with pytest.raises(contract.ContractError, match=r"missing b.*unexpected c"):
        contract.conform(spark.createDataFrame([(1, 1)], "a int, c int"), specs, "t")
