# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow import configuration as conf
# 
# # namespace = conf.get('kubernetes', 'NAMESPACE')
# namespace = 'pso-dev-e92439b19961-afl1'
# 
# default_args = {
#     'owner': 'Peter',
#     'depends_on_past': False,
#     'start_date': datetime.utcnow(),
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5)
# }
# 
# dag = DAG(
#     'example_kubernetes_2', default_args=default_args, schedule_interval=timedelta(minutes=10))
# 
# 
# start = DummyOperator(task_id='start', dag=dag)
# 
# passing = KubernetesPodOperator(namespace=namespace,
#                           image="python:3.6",
#                           cmds=["python","-c"],
#                           arguments=["print('hello world')"],
#                           labels={"foo": "bar"},
#                           name="passing-test",
#                           task_id="passing-task",
#                           is_delete_operator_pod=True,
#                           in_cluster=True,
#                           get_logs=True,
#                           dag=dag
#                           )
# 
# failing = KubernetesPodOperator(namespace=namespace,
#                           image="ubuntu:16.04",
#                           cmds=["python","-c"],
#                           arguments=["print('hello world')"],
#                           labels={"foo": "bar"},
#                           name="fail",
#                           task_id="failing-task",
#                           is_delete_operator_pod=True,
#                           in_cluster=True,
#                           get_logs=True,
#                           dag=dag
#                           )
# 
# end = DummyOperator(task_id='end', dag=dag)
# 
# 
# passing.set_upstream(start)
# failing.set_upstream(start)
# passing.set_downstream(end)
# failing.set_downstream(end)