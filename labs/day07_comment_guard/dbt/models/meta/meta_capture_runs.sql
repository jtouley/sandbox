{{ config(location=lake('meta', 'capture_runs')) }}

-- Meta: one row per `cg capture` run, from data/meta/runs/*.json manifests.
select
    run_id,
    status,
    error,
    started_at::timestamptz as started_at,
    finished_at::timestamptz as finished_at,
    datediff('second', started_at::timestamptz, finished_at::timestamptz) as duration_s,
    account ->> 'username' as account_username,
    account ->> 'user_id' as account_user_id,
    params,
    api_base,
    http_requests,
    http_failures,
    redactions,
    pages,
    items,
    len(files) as files,
    tool ->> 'version' as tool_version,
    runtime ->> 'python' as python_version,
    runtime ->> 'host' as host,
    filename as manifest_file
from read_json(
    '{{ var("data_dir") }}/meta/runs/*.json',
    filename = true,
    columns = {
        run_id: 'VARCHAR', status: 'VARCHAR', error: 'VARCHAR',
        started_at: 'VARCHAR', finished_at: 'VARCHAR',
        account: 'JSON', params: 'JSON', api_base: 'VARCHAR',
        http_requests: 'INTEGER', http_failures: 'INTEGER', redactions: 'INTEGER',
        pages: 'JSON', items: 'JSON',
        files: 'STRUCT(stream VARCHAR, path VARCHAR, bytes BIGINT, lines BIGINT, sha256 VARCHAR)[]',
        tool: 'JSON', runtime: 'JSON'
    }
)
