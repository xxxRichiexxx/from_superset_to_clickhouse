DROP TABLE IF EXISTS bi.ext_superset_logs ON CLUSTER '{{ params.cluster }}';
DROP TABLE IF EXISTS bi.ext_superset_ab_user ON CLUSTER '{{ params.cluster }}';
DROP TABLE IF EXISTS bi.ext_superset_dashboards ON CLUSTER '{{ params.cluster }}';