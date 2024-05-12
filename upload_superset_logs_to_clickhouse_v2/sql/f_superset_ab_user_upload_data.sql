INSERT INTO bi.f_superset_ab_user_distributed
    SELECT
        id,
        first_name,
        last_name,
        username,
        "password",
        active,
        email,
        last_login,
        login_count,
        fail_login_count,
        created_on,
        changed_on,
        created_by_fk,
        changed_by_fk
    FROM bi.ext_superset_ab_user
    WHERE {{ params.refrash_field }} > '{{ params.batch_start_date }}'
