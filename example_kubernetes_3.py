from datetime import datetime, timedelta

from airflow import DAG
from airflow import configuration as conf
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_args = {
    'owner': 'Peter',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
}

namespace = conf.get('kubernetes', 'NAMESPACE')

dag = DAG(
    dag_id='example_kubernetes_3',
    default_args=default_args
)

start = DummyOperator(
    name='start',
    task_id='start-id',
    dag=dag
)

kpo = KubernetesPodOperator(
    namespace=namespace,
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    name="kpo",
    task_id="kpo-id",
    is_delete_operator_pod=True,
    in_cluster=True,
    get_logs=True,
    dag=dag
)

end = DummyOperator(
    name='end',
    task_id='end-id', 
    dag=dag
)

start >> kpo >> end