from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators import BashOperator


default_args = {
    'owner':'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'example',
    default_args=default_args,
    description='Simple DAG',
    schedule_interval=timedelta(days=1)
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag,
)

templated_command = """
{% for i in range(5) %}
echo "{{ds}}"
echo "{{params.my_param}}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param':'Hello'},
    start_date=airflow.utils.dates.days_ago(1),
)

t1 >> t2
t2 >> t3