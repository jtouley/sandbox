from comment_guard.redact import REDACTED, redact_text, strip_secret_params


def test_redacts_token_and_query_params():
    text = '{"next":"https://x/y?access_token=SECRET123&after=a","other":"SECRET123"}'
    out, n = redact_text(text, "SECRET123")
    assert "SECRET123" not in out
    assert f"access_token={REDACTED}" in out
    assert n == 2


def test_counts_param_secrets_without_known_token():
    out, n = redact_text("a?access_token=abc&appsecret_proof=def&x=1")
    assert out == f"a?access_token={REDACTED}&appsecret_proof={REDACTED}&x=1"
    assert n == 2


def test_strip_secret_params_keeps_field_expansions():
    url = "https://g/v24.0/m1/comments?access_token=T&fields=id,replies.limit(50){id}&after=k"
    out = strip_secret_params(url)
    assert "access_token" not in out
    assert "replies.limit(50){id}" in out
    assert "after=k" in out
