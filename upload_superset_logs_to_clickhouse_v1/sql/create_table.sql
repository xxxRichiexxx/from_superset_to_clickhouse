DROP TABLE IF EXISTS bi.f_superset_logs_replicated ON CLUSTER 'ch-001';
CREATE TABLE IF NOT EXISTS bi.f_superset_logs_replicated ON CLUSTER 'ch-001'
(
    id INT NOT NULL,
	"action" Nullable(VARCHAR),
	user_id Nullable(INT),
	json Nullable(text),
	dttm DATETIME,
	dashboard_id Nullable(INT),
	slice_id Nullable(INT),
	duration_ms Nullable(INT),
	referrer Nullable(VARCHAR),
	source VARCHAR  NOT NULL
)
ENGINE = ReplicatedMergeTree
PARTITION BY date_trunc('month', dttm)
ORDER BY id;

DROP TABLE IF EXISTS bi.f_superset_logs_distributed ON CLUSTER 'ch-001';
CREATE TABLE IF NOT EXISTS bi.f_superset_logs_distributed ON CLUSTER 'ch-001'
AS bi.f_superset_logs_replicated
ENGINE = Distributed('ch-001', 'bi', 'f_superset_logs_replicated', id);