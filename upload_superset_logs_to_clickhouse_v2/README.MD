Данный даг предназначен для загрузки таблиц logs, ab_user, dashboards из Superset в Clickhouse.
Загрузка происходит инкрементами раз в день.
При первом запуске дага создаются таблицы в clickhouse для хранения данных и внешние таблицы для полкучения данных из
superset. Т.к таблицы для хранения пустые, то формируется инкремент данных с временем последнего изменения от 2000-01-01.
После первичной загрузки данных инкремент формируется относительно даты последнего изменения записей в обновляемой таблице.

По умолчанию используется подключение 'superset' (подключение к СУБД актуальной версии приложения). Для сбора данных из старого Superset'а необходимо при запуске дага указать:

    {"superset_connection": "superset_old"}

Порядок загрузки данных из разных версий superset'a следующий:

1. Запускаем даг вручную с параметром {"superset_connection": "superset_old"}.
        (Формируются таблицы в CH и наполняются данными из старого Superset'a)
2. Переименовываем следующие таблицы 
   
        bi.dct_superset_ab_user_replicated --> bi.dct_superset_ab_user_replicated_old
        bi.dct_superset_ab_user_distributed --> bi.dct_superset_ab_user_distributed_old
        bi.dct_superset_dashboards_replicated --> bi.dct_superset_dashboards_replicated_old
        bi.dct_superset_dashboards_distributed --> bi.dct_superset_dashboards_distributed_old
    
    Скрипт:

        RENAME TABLE bi.dct_superset_ab_user_replicated TO bi.dct_superset_ab_user_replicated_old ON CLUSTER 'ch-001';
        RENAME TABLE bi.dct_superset_ab_user_distributed TO bi.dct_superset_ab_user_distributed_old ON CLUSTER 'ch-001';
        RENAME TABLE bi.dct_superset_dashboards_replicated TO bi.dct_superset_dashboards_replicated_old ON CLUSTER 'ch-001';
        RENAME TABLE bi.dct_superset_dashboards_distributed  TO bi.dct_superset_dashboards_distributed_old ON CLUSTER 'ch-001';
    
3. Запускаем даг без параметров.
    Таблица bi.f_superset_logs_distributed наполняется логами из нового Superset'a, заново создаются 
    следующие таблицы с данными из нового Superset'a:

        bi.dct_superset_ab_user_replicated
        bi.dct_superset_ab_user_distributed
        bi.dct_superset_dashboards_replicated
        bi.dct_superset_dashboards_distributed

В итоге имеем единую таблицу с логами, старую и новую версию справочников.
Такой подход обусловлен тем, что справочники в старой и новой версии суперсета частично отличаются по идентификаторам, т.е
если на старых логах использовать новый справочник пользователей, то некоторым пользователям припишется чужая статистика (около 0.077% событий).
Если такое поведение допустимо, то можно не делать пункт 2 выше, и новые данные справочников вмержатся в уже имеющиеся таблицы с данными из старого суперсета.

P.S.
В рамках данной задачи внесены изменения в ClickHouseOperator. Данные изменения позволяют использовать в операторе шаблонные переменные.
Пример:

    arg2 = """
    {% if dag_run.conf.get('superset_connection') %}
        {{ dag_run.conf.get('superset_connection') }}
    {% else %}
        superset
    {% endif %}
    """.strip()

    test = ClickHouseOperator(
        task_id='test',
        sql="SELECT {{ params.arg1 }}, '{{ custom_template_fields.arg2 }}'",
        default_args=tasks_default_args,
        params={
            'arg1': 1,
        },
        custom_template_fields ={
            'arg2': arg2,
        }
    )

Новая версия оператора позволяет работать с контекстом и применять, если необходимо, условную логику в работе оператора. Т.е можно уйти от прямого использования clickhouse_hook.