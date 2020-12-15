from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper function
def my_function():
    from airflow.models import Variable
    catalogs_folder = Variable.get("CATALOGS_FOLDER")
    
    import numpy as np
    import pandas as pd
    from pplaa import Project
    
    prj = Project()
    prj.init(catalogs_folder + '/example_006')

    prj.cat.raw.pokemon.load()

    prj.cat.raw.pokemon.load()['HP'].max()  # Max HP

    validation_rules = {
        'raw.pokemon': {
            'rules': [
                {
                    'rtype': 'REQUIRED_COLUMNS_RULE',
                    'mandatory': 1,
                    'columns': [
                        'Name', 'Type 1', 'Total', 'HP'
                    ],
                    'strict': 0,
                    'paused': 0
                }, 
                {
                    'rtype': 'MIN_MAX_RULE',
                    'mandatory': 1,
                    'column': 'HP',
                    'min_value': 0,
                    'max_value': 255   # <-- Max HP
                }
            ]
        }
    }

    prj.cat.set_validation_rules(validation_rules)

    prj.cat.validate('raw.pokemon').passed

    # The cat.validate() method returns a ValidationReport object
    type(prj.cat.validate('raw.pokemon'))

    # When we print a ValidationReport, we obtain a report of the result
    print(prj.cat.validate('raw.pokemon'))

    # Forcing fail in the validation changing the max_value for HP (MIN_MAX_RULE)
    validation_rules['raw.pokemon']['rules'][1]['max_value'] = 254

    prj.cat.set_validation_rules(validation_rules)

    prj.cat.validate('raw.pokemon').passed

    print(prj.cat.validate('raw.pokemon'))

    # REQUIRED_COLUMNS_RULE -> Ok
    vars(prj.cat.validate('raw.pokemon').validation_result.results[0]['result'])

    # MIN_MAX_RULE -> Fail
    vars(prj.cat.validate('raw.pokemon').validation_result.results[1]['result'])

    print('Padrino, llegue !!!')

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Dag
dag = DAG(
    dag_id='example_006',
    default_args=args,
    description='A simple DAG',
)

# Task
my_task = PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    dag=dag,
)