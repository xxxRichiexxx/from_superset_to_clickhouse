INSERT INTO bi.f_superset_logs_distributed
    SELECT
        id,
        "action",
        user_id,
        json,
        dttm,
        dashboard_id,
        slice_id,
        duration_ms,
        referrer,
        '{{ params.src_db }}'
    FROM bi.ext_superset_logs
    WHERE {{ params.refrash_field }} > '{{ params.batch_start_date }}';