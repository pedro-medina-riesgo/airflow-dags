# from kubernetes.client import models as k8s
# 
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.kubernetes.secret import Secret
# from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
# 
# namespace = 'pso-dev-e92439b19961-afl1'
# 
# default_args = {
#     'owner': 'Peter',
#     'depends_on_past': False,
#     'start_date': days_ago(2),
# }
# 
# dag = DAG(
#     dag_id='example_kubernetes_1',
#     default_args=default_args,
# ) 
# 
# k = KubernetesPodOperator(
#     namespace=namespace,
#     image="ubuntu:16.04",
#     cmds=["bash", "-cx"],
#     arguments=["echo", "10"],
#     name="k-name",
#     task_id="k-task",
#     is_delete_operator_pod=True,
#     in_cluster=True,
#     get_logs=True,
#     dag=dag
# )





    ##resources={'request_memory': '3Gi',
    ##            'request_cpu': '200m',
    ##            'limit_memory': '3Gi',
    ##            'limit_cpu': 1},
    ##image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
    ##tolerations=Tolerations.default,
    ##affinity=Affinity.memory_heavy,
    ##startup_timeout_seconds=60,

#write_xcom = KubernetesPodOperator(
#    namespace=namespace,
#    image='alpine',
#    cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
#    name="write_xcom-name",
#    task_id="write_xcom-task",
#    do_xcom_push=True,
#    is_delete_operator_pod=True,
#    in_cluster=True,
#    get_logs=True,
#    dag=dag
#)

#pod_task_xcom_result = BashOperator(
#    bash_command="echo \"{{ task_instance.xcom_pull('k-task')[0] }}\"",
#    name="pod_task_xcom_result-name",
#    task_id="pod_task_xcom_result-task",
#    dag=dag,
#)