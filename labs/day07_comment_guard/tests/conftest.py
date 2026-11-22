"""A fake Instagram Graph API (via respx) with paging, nested replies, and a replies edge."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from comment_guard.config import GRAPH_HOST, Settings

TOKEN = "IGAAtestTOKEN1234567890abcdef"
VERSION = "v24.0"
BASE = f"{GRAPH_HOST}/{VERSION}"


def _next(path: str, after: str) -> str:
    # Meta's real paging URLs carry the token in the query string.
    return f"{BASE}/{path}?access_token={TOKEN}&limit=50&after={after}"


ME = {
    "id": "app-scoped-1",
    "user_id": "17840000000000001",
    "username": "test_creator",
    "account_type": "MEDIA_CREATOR",
    "media_count": 3,
}

MEDIA_PAGE_1 = {
    "data": [
        {
            "id": "m1",
            "permalink": "https://www.instagram.com/p/AAA/",
            "caption": "Ride day",
            "timestamp": "2026-11-20T15:00:00+0000",
            "comments_count": 4,
            "media_type": "VIDEO",
        },
        {
            "id": "m2",
            "permalink": "https://www.instagram.com/p/BBB/",
            "caption": "Bike lane",
            "timestamp": "2026-11-18T15:00:00+0000",
            "comments_count": 1,
            "media_type": "IMAGE",
        },
    ],
    "paging": {"cursors": {"after": "c1"}, "next": _next("me/media", "c1")},
}
MEDIA_PAGE_2 = {
    "data": [
        {
            "id": "m3",
            "permalink": "https://www.instagram.com/p/CCC/",
            "caption": "Old post",
            "timestamp": "2026-10-01T15:00:00+0000",
            "comments_count": 0,
            "media_type": "IMAGE",
        },
    ],
    "paging": {"cursors": {"after": "c2"}},
}


def _comment(cid, text, user, uid, *, ts="2026-11-20T16:00:00+0000", hidden=False, **kw):
    return {
        "id": cid,
        "text": text,
        "username": user,
        "from": {"id": uid, "username": user},
        "timestamp": ts,
        "like_count": 0,
        "hidden": hidden,
        **kw,
    }


M1_COMMENTS_1 = {
    "data": [
        _comment(
            "c100",
            "Great ride!",
            "fan_one",
            "u1",
            replies={
                "data": [_comment("c101", "agreed", "fan_two", "u2", parent_id="c100")],
                "paging": {"next": _next("c100/replies", "r1")},
            },
        ),
        _comment(
            "c200", "get off the road or I'll run you over", "angry_driver", "u9", hidden=True
        ),
    ],
    "paging": {"cursors": {"after": "k1"}, "next": _next("m1/comments", "k1")},
}
M1_COMMENTS_2 = {"data": [_comment("c300", "nice", "fan_one", "u1")], "paging": {}}
C100_REPLIES = {
    "data": [_comment("c102", "same", "fan_three", "u3", parent_id="c100")],
    "paging": {},
}
M2_COMMENTS = {"data": [_comment("c400", "share the road", "fan_two", "u2")], "paging": {}}


def install_routes(router: respx.MockRouter) -> None:
    def media(request: httpx.Request) -> httpx.Response:
        page = MEDIA_PAGE_2 if request.url.params.get("after") == "c1" else MEDIA_PAGE_1
        return httpx.Response(
            200, json=page, headers={"x-app-usage": '{"call_count":1}', "x-fb-trace-id": "TRACE"}
        )

    def m1(request: httpx.Request) -> httpx.Response:
        page = M1_COMMENTS_2 if request.url.params.get("after") == "k1" else M1_COMMENTS_1
        return httpx.Response(200, json=page)

    router.get(f"{BASE}/me").mock(return_value=httpx.Response(200, json=ME))
    router.get(f"{BASE}/me/media").mock(side_effect=media)
    router.get(f"{BASE}/m1/comments").mock(side_effect=m1)
    router.get(f"{BASE}/m2/comments").mock(return_value=httpx.Response(200, json=M2_COMMENTS))
    router.get(f"{BASE}/m3/comments").mock(
        return_value=httpx.Response(200, json={"data": [], "paging": {}})
    )
    router.get(f"{BASE}/c100/replies").mock(return_value=httpx.Response(200, json=C100_REPLIES))


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("COMMENT_GUARD_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("COMMENT_GUARD_DATA", str(tmp_path / "data"))
    monkeypatch.setenv("COMMENT_GUARD_TOKEN_BACKEND", "file")
    monkeypatch.setenv("IG_API_VERSION", VERSION)
    monkeypatch.delenv("IG_ACCESS_TOKEN", raising=False)
    return Settings.from_env()


@pytest.fixture
def graph():
    with respx.mock(assert_all_called=False) as router:
        install_routes(router)
        yield router


def read_lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]
