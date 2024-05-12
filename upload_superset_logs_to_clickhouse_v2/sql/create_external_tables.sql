DROP TABLE IF EXISTS bi.ext_superset_logs ON CLUSTER '{{ params.cluster }}';
CREATE TABLE IF NOT EXISTS bi.ext_superset_logs ON CLUSTER '{{ params.cluster }}'
(
    id INT NOT NULL,
	"action" Nullable(VARCHAR),
	user_id Nullable(INT),
	json Nullable(text),
	dttm Nullable(DATETIME),
	dashboard_id Nullable(INT),
	slice_id Nullable(INT),
	duration_ms Nullable(INT),
	referrer Nullable(VARCHAR)
)
ENGINE=PostgreSQL('{{ params.src_host }}:{{ params.src_port }}', '{{ params.src_db }}', 'logs', '{{ params.src_login }}', '{{ params.src_password }}');

DROP TABLE IF EXISTS bi.ext_superset_ab_user ON CLUSTER '{{ params.cluster }}';
CREATE TABLE IF NOT EXISTS bi.ext_superset_ab_user ON CLUSTER '{{ params.cluster }}'
AS bi.f_superset_ab_user_replicated
ENGINE=PostgreSQL('{{ params.src_host }}:{{ params.src_port }}', '{{ params.src_db }}', 'ab_user', '{{ params.src_login }}', '{{ params.src_password }}');

DROP TABLE IF EXISTS bi.ext_superset_dashboards ON CLUSTER '{{ params.cluster }}';
CREATE TABLE IF NOT EXISTS bi.ext_superset_dashboards ON CLUSTER '{{ params.cluster }}'
AS bi.f_superset_dashboards_replicated
ENGINE=PostgreSQL('{{ params.src_host }}:{{ params.src_port }}', '{{ params.src_db }}', 'dashboards', '{{ params.src_login }}', '{{ params.src_password }}');