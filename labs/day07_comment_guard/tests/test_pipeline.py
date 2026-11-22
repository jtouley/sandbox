"""End to end: mocked capture (twice) -> real dbt build -> query the gold Parquet."""

import subprocess
import sys

import duckdb
import httpx
import pytest

from comment_guard.capture import run_capture
from comment_guard.cli import main
from comment_guard.client import GraphClient

from .conftest import BASE, M1_COMMENTS_1, TOKEN, VERSION


@pytest.fixture
def two_runs(settings, graph):
    with GraphClient(TOKEN, VERSION, sleep=lambda _s: None, min_interval=0) as c:
        run_capture(settings, c, max_posts=None)
    # Second run: the threatening comment c200 is gone (deleted / restricted).
    page = dict(M1_COMMENTS_1, data=[d for d in M1_COMMENTS_1["data"] if d["id"] != "c200"])
    empty = {"data": [], "paging": {}}
    graph.get(f"{BASE}/m1/comments").mock(
        side_effect=lambda req: httpx.Response(
            200, json=empty if req.url.params.get("after") == "k1" else page
        )
    )
    with GraphClient(TOKEN, VERSION, sleep=lambda _s: None, min_interval=0) as c:
        run_capture(settings, c, max_posts=None)
    return settings


def test_dbt_build_produces_medallion(two_runs):
    settings = two_runs
    # Out of process, exactly as a user runs it (and so dbt releases warehouse.duckdb).
    subprocess.run([sys.executable, "-m", "comment_guard.cli", "build"], check=True)  # noqa: S603

    con = duckdb.connect()
    lake = settings.lake
    q = lambda sql: con.sql(sql.format(lake=lake)).fetchall()  # noqa: E731

    assert q("select count(*) from '{lake}/bronze/api_responses.parquet'")[0][0] == 16
    assert q("select bool_and(body_hash_ok) from '{lake}/bronze/api_responses.parquet'")[0][0]

    rows = dict(
        q("select comment_id, possibly_removed from '{lake}/gold/comment_evidence.parquet'")
    )
    assert set(rows) == {"c100", "c101", "c102", "c200", "c300", "c400"}
    assert rows["c200"] is True  # vanished in run 2 -> flagged, but still preserved
    assert rows["c100"] is False

    ev = q(
        "select is_reply, parent_comment_id, ever_hidden, first_evidence_file "
        "from '{lake}/gold/comment_evidence.parquet' where comment_id = 'c200'"
    )[0]
    assert ev[0] is False and ev[1] is None and ev[2] is True
    assert ev[3].endswith("comments.jsonl")

    replies = q(
        "select comment_id, parent_comment_id from '{lake}/silver/comments.parquet' "
        "where is_reply order by 1"
    )
    assert replies == [("c101", "c100"), ("c102", "c100")]

    cov = dict(q("select media_id, comments_captured from '{lake}/gold/capture_coverage.parquet'"))
    assert cov == {"m1": 5, "m2": 1, "m3": 0}

    runs = q("select count(*), bool_and(status = 'ok') from '{lake}/meta/capture_runs.parquet'")
    assert runs[0] == (2, True)
    assert q("select bool_and(file_intact) from '{lake}/meta/landing_files.parquet'")[0][0]

    wh = duckdb.connect(str(settings.data / "warehouse.duckdb"), read_only=True)
    assert wh.execute("select count(*) from meta.dbt_node_runs").fetchall()[0][0] > 0
    # Every layer is also queryable as a view in the warehouse.
    assert wh.execute("select count(*) from gold.gld_comment_evidence").fetchall()[0][0] == 6


def test_tampering_fails_the_build(two_runs):
    settings = two_runs
    edited = 0
    for f in settings.landing.rglob("comments.jsonl"):
        text = f.read_text()
        if "run you over" in text:
            f.write_text(text.replace("run you over", "say hello"))
            edited += 1
    assert edited == 1
    assert main(["build"]) == 1
