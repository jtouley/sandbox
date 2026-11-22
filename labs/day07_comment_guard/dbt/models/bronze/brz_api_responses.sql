{{ config(location=lake('bronze', 'api_responses')) }}

-- Bronze: one row per landed API response, exactly as captured (body stays a string).
-- `body_hash_ok` re-verifies every line's SHA-256 so tampering or corruption fails the build.
select
    run_id,
    seq,
    stream,
    media_id,
    parent_comment_id,
    captured_at,
    request ->> 'method' as http_method,
    request ->> 'url' as request_url,
    status_code,
    elapsed_ms,
    headers,
    headers ->> 'x-fb-trace-id' as fb_trace_id,
    headers ->> 'x-app-usage' as app_usage,
    redactions,
    body_sha256,
    sha256(body) = body_sha256 as body_hash_ok,
    body,
    filename as landing_file,
    schema_version
from read_ndjson(
    '{{ landing_glob() }}',
    filename = true,
    columns = {
        schema_version: 'INTEGER',
        run_id: 'VARCHAR',
        seq: 'BIGINT',
        stream: 'VARCHAR',
        media_id: 'VARCHAR',
        parent_comment_id: 'VARCHAR',
        captured_at: 'TIMESTAMPTZ',
        request: 'JSON',
        status_code: 'INTEGER',
        elapsed_ms: 'DOUBLE',
        headers: 'JSON',
        redactions: 'INTEGER',
        body_sha256: 'VARCHAR',
        body: 'VARCHAR'
    }
)
