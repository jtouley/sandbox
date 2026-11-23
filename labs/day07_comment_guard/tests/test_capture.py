import hashlib
import json

import httpx
import pytest
import respx

from comment_guard.capture import run_capture
from comment_guard.client import GraphClient, GraphError

from .conftest import BASE, TOKEN, VERSION, read_lines


def _client():
    return GraphClient(TOKEN, VERSION, sleep=lambda _s: None, min_interval=0)


def test_capture_lands_every_response(settings, graph):
    with _client() as client:
        manifest = run_capture(settings, client, max_posts=None)

    assert manifest["status"] == "ok"
    assert manifest["items"] == {"media": 3, "comments": 4, "replies": 2, "comment_lines": 6}
    # me + 2 media pages + m1 (2 pages) + m2 + m3 + c100 replies
    assert manifest["http_requests"] == 8
    streams = {f["stream"] for f in manifest["files"]}
    assert streams == {"me", "media", "comments", "replies", "comment_lines"}

    for f in manifest["files"]:
        if f["stream"] == "comment_lines":
            continue
        path = settings.data / f["path"]
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == f["sha256"]
        assert TOKEN.encode() not in raw, f"token leaked into {path}"
        for env in read_lines(path):
            assert hashlib.sha256(env["body"].encode()).hexdigest() == env["body_sha256"]
            assert env["run_id"] == manifest["run_id"]

    assert manifest["redactions"] > 0
    assert (settings.runs / f"{manifest['run_id']}.json").exists()
    assert not list(settings.landing.rglob("*.part"))


def test_token_sent_in_header_not_url(settings, graph):
    with _client() as client:
        run_capture(settings, client, max_posts=1)
    for call in graph.calls:
        assert TOKEN not in str(call.request.url)
        assert call.request.headers["authorization"] == f"Bearer {TOKEN}"


def test_max_posts_limits_comment_fetches(settings, graph):
    with _client() as client:
        manifest = run_capture(settings, client, max_posts=1)
    assert manifest["items"]["media"] == 1
    called = {c.request.url.path for c in graph.calls}
    assert f"/{VERSION}/m2/comments" not in called


def test_failed_run_still_writes_manifest_and_landing(settings, graph):
    graph.get(f"{BASE}/m2/comments").mock(
        return_value=httpx.Response(400, json={"error": {"code": 100, "message": "bad field"}})
    )
    with _client() as client, pytest.raises(GraphError):
        run_capture(settings, client, max_posts=None)
    manifest = json.loads(next(settings.runs.glob("*.json")).read_text())
    assert manifest["status"] == "failed"
    assert manifest["http_failures"] == 1
    statuses = [
        e["status_code"]
        for e in read_lines(
            settings.data / next(f["path"] for f in manifest["files"] if f["stream"] == "comments")
        )
    ]
    assert 400 in statuses


def test_retries_rate_limits(settings, graph):
    route = graph.get(f"{BASE}/me")
    route.side_effect = [
        httpx.Response(400, json={"error": {"code": 4, "message": "Application request limit"}}),
        httpx.Response(200, json={"id": "x", "username": "test_creator"}),
    ]
    with _client() as client:
        assert client.get("me").json()["username"] == "test_creator"
        assert client.requests == 2


def test_one_line_per_comment_grouped_by_post(settings, graph):
    with _client() as client:
        manifest = run_capture(settings, client, max_posts=None)

    line_files = {
        f["path"].rsplit("/", 1)[-1]: f for f in manifest["files"] if f["stream"] == "comment_lines"
    }
    assert set(line_files) == {"post_m1.jsonl", "post_m2.jsonl"}  # m3 has no comments
    m1 = read_lines(settings.data / line_files["post_m1.jsonl"]["path"])
    # Threaded: each top-level comment, then its replies oldest-first.
    assert [(x["comment_id"], x["parent_comment_id"]) for x in m1] == [
        ("c100", None),
        ("c101", "c100"),
        ("c102", "c100"),
        ("c200", None),
        ("c300", None),
    ]
    threat = m1[3]
    assert threat["text"] == "get off the road or I'll run you over"
    assert threat["username"] == "angry_driver" and threat["user_id"] == "u9"
    assert threat["hidden"] is True and threat["is_reply"] is False
    assert threat["post_permalink"] == "https://www.instagram.com/p/AAA/"

    # Every line points at the raw response it came from, and that pointer checks out.
    raw = read_lines(settings.data / threat["source"]["file"])
    src = next(e for e in raw if e["seq"] == threat["source"]["seq"])
    assert src["body_sha256"] == threat["source"]["body_sha256"]
    assert "run you over" in src["body"]
    assert TOKEN not in (settings.data / line_files["post_m1.jsonl"]["path"]).read_text()


def test_header_auth_falls_back_to_query_param(settings):
    calls = []

    def me(request):
        calls.append(request)
        if "authorization" in request.headers:
            return httpx.Response(
                400, json={"error": {"code": 190, "message": "Invalid OAuth access token"}}
            )
        assert request.url.params["access_token"] == TOKEN
        return httpx.Response(200, json={"username": "test_creator"})

    with respx.mock() as router, _client() as client:
        router.get(f"{BASE}/me").mock(side_effect=me)
        assert client.get("me").json()["username"] == "test_creator"
        assert client.auth_mode == "query"
    assert len(calls) == 2
