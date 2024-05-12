-- logs --
CREATE TABLE IF NOT EXISTS bi.f_superset_logs_replicated ON CLUSTER '{{ params.cluster }}'
(
    id INT NOT NULL,
	"action" VARCHAR DEFAULT 'undefined',
	user_id INT DEFAULT -1,
	json TEXT DEFAULT 'undefined',
	dttm DATETIME NOT NULL,
	dashboard_id INT DEFAULT -1,
	slice_id INT DEFAULT -1,
	duration_ms INT DEFAULT 0,
	referrer VARCHAR DEFAULT 'undefined',
	source VARCHAR  NOT NULL
)
ENGINE = ReplicatedReplacingMergeTree
PARTITION BY date_trunc('month', dttm)
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS bi.f_superset_logs_distributed ON CLUSTER '{{ params.cluster }}'
AS bi.f_superset_logs_replicated
ENGINE = Distributed('{{ params.cluster }}', 'bi', 'f_superset_logs_replicated', id);

-- ab_user --
CREATE TABLE IF NOT EXISTS bi.f_superset_ab_user_replicated ON CLUSTER '{{ params.cluster }}'
(
	id INT NOT NULL,
	first_name VARCHAR NOT NULL,
	last_name VARCHAR NOT NULL,
	username VARCHAR NOT NULL,
	"password" VARCHAR DEFAULT 'undefined',
	active BOOL DEFAULT 'false',
	email VARCHAR NOT NULL,
	last_login DATETIME DEFAULT '1997-01-01',
	login_count INT DEFAULT 0,
	fail_login_count INT DEFAULT 0,
	created_on DATETIME DEFAULT '1997-01-01',
	changed_on DATETIME DEFAULT '1997-01-01',
	created_by_fk INT DEFAULT -1,
	changed_by_fk INT DEFAULT -1
)
ENGINE = ReplicatedReplacingMergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS bi.f_superset_ab_user_distributed ON CLUSTER '{{ params.cluster }}'
AS bi.f_superset_ab_user_replicated
ENGINE = Distributed('{{ params.cluster }}', 'bi', 'f_superset_ab_user_replicated', id);

DROP DICTIONARY IF EXISTS bi.dct_superset_ab_user ON CLUSTER '{{ params.cluster }}';
CREATE DICTIONARY IF NOT EXISTS bi.dct_superset_ab_user ON CLUSTER '{{ params.cluster }}'
(
    id INT,
    first_name VARCHAR,
    last_name VARCHAR,
	username VARCHAR,
	active BOOL,
	email VARCHAR	
)
PRIMARY KEY id
SOURCE (
    CLICKHOUSE (
        host 'localhost'
        db 'bi'
        table 'f_superset_ab_user_distributed'
    )
)
LIFETIME (43200)
LAYOUT (hashed());

-- dashboards --
CREATE TABLE IF NOT EXISTS bi.f_superset_dashboards_replicated ON CLUSTER '{{ params.cluster }}'
(
	created_on DATETIME ,
	changed_on DATETIME DEFAULT '1997-01-01',
	id INT NOT NULL,
	dashboard_title VARCHAR DEFAULT 'undefined',
	position_json VARCHAR DEFAULT 'undefined',
	created_by_fk INT DEFAULT -1,
	changed_by_fk INT DEFAULT -1,
	css VARCHAR DEFAULT 'undefined',
	description VARCHAR DEFAULT 'undefined',
	slug VARCHAR DEFAULT 'undefined',
	json_metadata VARCHAR DEFAULT 'undefined',
	published BOOL DEFAULT 'false',
	"uuid" UUID DEFAULT '00000000-0000-0000-0000-000000000000',
	certified_by VARCHAR DEFAULT 'undefined',
	certification_details VARCHAR DEFAULT 'undefined',
	is_managed_externally BOOL DEFAULT 'false',
	external_url VARCHAR DEFAULT 'undefined'
)
ENGINE = ReplicatedReplacingMergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS bi.f_superset_dashboards_distributed ON CLUSTER '{{ params.cluster }}'
AS bi.f_superset_dashboards_replicated
ENGINE = Distributed('{{ params.cluster }}', 'bi', 'f_superset_dashboards_replicated', id);

DROP DICTIONARY IF EXISTS bi.dct_superset_dashboards ON CLUSTER '{{ params.cluster }}';
CREATE DICTIONARY IF NOT EXISTS bi.dct_superset_dashboards ON CLUSTER '{{ params.cluster }}'
(
    id INT,
    dashboard_title VARCHAR,
	created_on DATETIME,
	published BOOL
)
PRIMARY KEY id
SOURCE (
    CLICKHOUSE (
        host 'localhost'
        db 'bi'
        table 'f_superset_dashboards_distributed'
    )
)
LIFETIME (43200)
LAYOUT (hashed());