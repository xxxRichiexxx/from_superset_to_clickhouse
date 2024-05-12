--Данные скрипты не запускаются ДАГом, нужны для очистки за собой тестовой среды.
DROP TABLE IF EXISTS bi.f_superset_logs_replicated ON CLUSTER '{{ params.cluster }}';
DROP TABLE IF EXISTS bi.f_superset_logs_distributed ON CLUSTER '{{ params.cluster }}';
DROP TABLE IF EXISTS bi.f_superset_ab_user_replicated ON CLUSTER '{{ params.cluster }}';
DROP TABLE IF EXISTS bi.f_superset_ab_user_distributed ON CLUSTER '{{ params.cluster }}';
DROP TABLE IF EXISTS bi.f_superset_dashboards_replicated ON CLUSTER '{{ params.cluster }}';
DROP TABLE IF EXISTS bi.f_superset_dashboards_distributed ON CLUSTER '{{ params.cluster }}';
DROP DICTIONARY IF EXISTS bi.dct_superset_ab_user;
DROP DICTIONARY IF EXISTS bi.dct_superset_dashboards;