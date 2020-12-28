from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


from datetime import datetime
print(datetime.now())

a = [1, 2, 3]   # Por omision, global
b = [1, 2, 3]   # Explicitamente global
c = [1, 2, 3]   # Parametro
d = [1, 2, 3]   # Contexto

# Wrapper function
def test_1(c, **kwargs):
    global b

    print(a)
    print(b)
    print(c)
    print(kwargs['dag'].params['d'])

    a.append(4)
    b.append(4)
    c.append(4)
    kwargs['dag'].params['d'].append(4)

    print(a)
    print(b)
    print(c)
    print(kwargs['dag'].params['d'])

    print(id(a))
    print(id(b))
    print(id(c))
    print(id(kwargs['dag'].params['d']))

def test_2(c, **kwargs):

    print(a)
    print(b)
    print(c)
    print(kwargs['dag'].params['d'])

    print(id(a))
    print(id(b))
    print(id(c))
    print(id(kwargs['dag'].params['d']))

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'params': {'d': d}
}

# Dag
dag = DAG(
    dag_id='example_005',
    default_args=args,
    description='A simple DAG',
)

# Task
t_005_1 = PythonOperator(
    task_id='t_005_1',
    python_callable=test_1,
    op_kwargs={'c': c},
    provide_context=True,
    dag=dag,
)

t_005_2 = PythonOperator(
    task_id='t_005_2',
    python_callable=test_2,
    op_kwargs={'c': c},
    provide_context=True,
    dag=dag,
)

t_005_1 >> t_005_2