INSERT INTO bi.f_superset_dashboards_distributed
    SELECT
        created_on,
        changed_on,
        id,
        dashboard_title,
        position_json,
        created_by_fk,
        changed_by_fk,
        css,
        description,
        slug,
        json_metadata,
        published,
        "uuid",
        certified_by,
        certification_details,
        is_managed_externally,
        external_url
    FROM bi.ext_superset_dashboards
    WHERE {{ params.refrash_field }} > '{{ params.batch_start_date }}';