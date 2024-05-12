DROP TABLE IF EXISTS bi.f_superset_logs_processed ON cluster 'ch-001';
CREATE TABLE bi.f_superset_logs_processed ON cluster  'ch-001'
AS bi.f_superset_logs_replicated
engine = ReplicatedMergeTree
PARTITION BY date_trunc('month', dttm)
ORDER BY id;

DROP TABLE IF EXISTS bi.ext_superset_logs ON CLUSTER 'ch-001';
CREATE TABLE IF NOT EXISTS bi.ext_superset_logs ON CLUSTER 'ch-001'
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
ENGINE=PostgreSQL('{{ params.src_host }}:{{ params.src_port }}', '{{ params.src_bd }}', '{{ params.src_table }}', '{{ params.src_login }}', '{{ params.src_password }}');
