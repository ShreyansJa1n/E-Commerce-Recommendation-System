from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from recsys.features.events import sessionize
from recsys.features.item import build_item_features
from recsys.features.split import Cutoff
from recsys.features.user import build_user_category_affinity, build_user_features
from tests.feature_helpers import categories, enriched, feature_config, scd, to_ms

T = datetime(2015, 6, 30)
CUT = Cutoff("train", date(2015, 6, 30), date(2015, 7, 14))
FC = feature_config()  # windows 7d/30d, weights view=1 cart=3 txn=5, prior 20


@pytest.fixture
def events(spark: SparkSession):
    d = lambda day, h=12: datetime(2015, 6, day, h)  # noqa: E731
    return enriched(
        spark,
        [
            # visitor 1: old view (outside 30d), then recent activity in 2 sessions
            (datetime(2015, 5, 1), 1, 100, "view", None, "1:1", 10),
            (d(25), 1, 100, "view", None, "1:2", 10),
            (d(25, 13), 1, 101, "view", None, "1:2", 10),
            (d(26), 1, 101, "addtocart", None, "1:3", 10),
            (d(29), 1, 101, "transaction", 7, "1:4", 10),
            (d(29), 1, 102, "transaction", 7, "1:4", 20),
            # visitor 2: views only
            (d(10), 2, 100, "view", None, "2:1", 10),
            (d(28), 2, 102, "view", None, "2:2", 20),
            # at/after T: must be ignored
            (T, 1, 100, "transaction", 8, "1:5", 10),
            (datetime(2015, 7, 2), 3, 100, "view", None, "3:1", 10),
        ],
    )


def test_user_features_hand_computed(events) -> None:
    rows = {r.visitor_id: r for r in build_user_features(events, CUT, FC).collect()}
    assert set(rows) == {1, 2}  # visitor 3 only has future events
    u = rows[1]
    assert u.days_since_first_event == pytest.approx(60.0)
    assert u.days_since_last_event == pytest.approx(0.5)
    assert u.days_since_last_view == pytest.approx(4.5 - 1 / 24)
    assert u.n_events_total == 6 and u.n_sessions_total == 4
    assert (u.n_views_7d, u.n_addtocart_7d, u.n_transactions_7d) == (2, 1, 2)
    assert (u.n_views_30d, u.n_sessions_30d, u.n_distinct_items_30d) == (2, 3, 3)
    assert u.view_to_cart_rate_7d == pytest.approx(0.5)
    assert u.cart_to_purchase_rate_7d == 1.0  # 2/1 clipped
    # affinity 30d: cat 10 = view+view+cart+txn = 1+1+3+5 = 10, cat 20 = txn = 5
    assert (u.n_categories_30d, u.top_category_id_30d) == (2, 10)
    assert u.top_category_share_30d == pytest.approx(10 / 15)
    v = rows[2]
    assert v.n_views_7d == 1 and v.n_views_30d == 2
    assert v.view_to_cart_rate_30d == 0.0
    assert v.cart_to_purchase_rate_30d is None  # no carts
    assert v.days_since_last_transaction is None


def test_affinity_shares_sum_to_one(events) -> None:
    aff = build_user_category_affinity(events, CUT, FC).collect()
    by_user: dict[int, float] = {}
    for r in aff:
        by_user[r.visitor_id] = by_user.get(r.visitor_id, 0.0) + r.affinity_share
    assert by_user == pytest.approx({1: 1.0, 2: 1.0})


def test_item_features_hand_computed(spark: SparkSession, events) -> None:
    catalog = scd(
        spark,
        [
            (100, "categoryid", "10", datetime(1970, 1, 1), None),
            (101, "categoryid", "10", datetime(1970, 1, 1), datetime(2015, 7, 1)),
            (101, "categoryid", "20", datetime(2015, 7, 1), None),  # change after T
            (102, "categoryid", "20", datetime(1970, 1, 1), None),
            (102, "available", "1", datetime(2015, 6, 1), None),
            (200, "categoryid", "20", datetime(2015, 6, 1), None),  # catalog-only item
            (300, "categoryid", "20", datetime(2015, 7, 5), None),  # appears after T
        ],
    )
    cats = categories(spark, [(10, None, 10, 0), (20, 10, 10, 1)])
    rows = {r.item_id: r for r in build_item_features(events, catalog, cats, CUT, FC).collect()}
    assert set(rows) == {100, 101, 102, 200}
    assert rows[101].category_id == 10  # value at T, not the later change
    assert (rows[102].available, rows[102].category_level) == (1, 1)
    i = rows[100]
    assert (i.n_views_30d, i.n_visitors_30d, i.n_views_7d) == (2, 2, 1)
    assert i.coview_neighbors_30d == 1  # 100 & 101 in session 1:2
    assert rows[101].copurchase_neighbors_total == 1  # 101 & 102 in txn 7
    # weighted 30d scores: 101 = 1+3+5 = 9, 102 = 5+1 = 6, 100 = 2 -> ranks 1, 2, 3
    assert (rows[101].popularity_rank_30d, rows[102].popularity_rank_30d) == (1, 2)
    assert rows[100].popularity_rank_30d == 3
    cold = rows[200]
    assert cold.n_views_30d == 0 and cold.popularity_rank_30d is None
    assert cold.days_since_last_event is None
    # smoothed: global cart rate (30d) = 1 cart / 4 views; cold item = prior
    assert cold.cart_rate_30d == pytest.approx(0.25)
    assert rows[101].cart_rate_30d == pytest.approx((1 + 20 * 0.25) / (1 + 20))


def test_sessionize_gap(spark: SparkSession) -> None:
    rows = [
        (datetime(2015, 6, 1, 10, 0), 1),
        (datetime(2015, 6, 1, 10, 30), 1),  # exactly 30 min: same session
        (datetime(2015, 6, 1, 11, 0, 1), 1),  # > 30 min: new session
        (datetime(2015, 6, 1, 10, 0), 2),
    ]
    df = spark.createDataFrame(
        [(to_ms(t), v, "view", 1) for t, v in rows],
        "ts_ms bigint, visitor_id int, event_type string, item_id int",
    )
    got = sorted((r.visitor_id, r.ts_ms, r.session_id) for r in sessionize(df, 30).collect())
    assert [s for _, _, s in got] == ["1:1", "1:1", "1:2", "2:1"]
