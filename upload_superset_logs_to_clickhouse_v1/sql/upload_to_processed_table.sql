INSERT INTO bi.f_superset_logs_processed
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
        '{{ params.src_bd }}'
    FROM bi.ext_superset_logs
    WHERE dttm >= '{{ params.batch_start_date }}' AND {{ params.where_conditions }}
    limit 1000;