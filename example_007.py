from airflow.models import DAG 
from airflow.utils.dates import days_ago 
from airflow.operators.python_operator import PythonOperator 


# Catalog code
from airflow.models import Variable
catalogs_folder = Variable.get("CATALOGS_FOLDER")

from pplaa import Project
prj = Project()
prj.init(catalogs_folder + '/example_007')


# Notebook code 
#!/usr/bin/env python
# coding: utf-8

import warnings
warnings.filterwarnings("ignore")


# Wrapper functions 
def preparation_wrapper_function():
#def preparation_wrapper_function(prj):
    df = prj.cat.raw.locations.load()
    df = df.head(500)
    prj.cat.save(data=df, dsid='intermediate.locations')


def first_validation_wrapper_function():
#def first_validation_wrapper_function(prj):
    prj.cat.intermediate.locations.load()

    validation_rules = {
        'intermediate.locations': {
            'rules': [
                {
                    'rtype': 'REQUIRED_COLUMNS_RULE',
                    'mandatory': 1,
                    'columns': [
                        'XCOORDINATE', 'YCOORDINATE', 'LOCATION'
                    ],
                    'strict': 0,
                    'paused': 0
                }, 
                {
                    'rtype': 'MIN_MAX_RULE',
                    'mandatory': 1,
                    'column': 'XCOORDINATE',
                    'min_value': 0,
                    'max_value': 500
                }
            ]
        }
    }

    prj.cat.set_validation_rules(validation_rules)
    print(prj.cat.validate('intermediate.locations'))
    result = prj.cat.validate('intermediate.locations')
    assert result.passed, str(result)


def second_validation_wrapper_function():
#def second_validation_wrapper_function(prj):
    prj.cat.intermediate.locations.load()

    validation_rules = {
        'intermediate.locations': {
            'rules': [
                {
                    'rtype': 'REQUIRED_COLUMNS_RULE',
                    'mandatory': 1,
                    'columns': [
                        'XCOORDINATE', 'YCOORDINATE', 'LOCATION'
                    ],
                    'strict': 0,
                    'paused': 0
                }, 
                {
                    'rtype': 'MIN_MAX_RULE',
                    'mandatory': 1,
                    'column': 'XCOORDINATE',
                    'min_value': 58,
                    'max_value': 58
                }
            ]
        }
    }

    prj.cat.set_validation_rules(validation_rules)
    print(prj.cat.validate('intermediate.locations'))
    result = prj.cat.validate('intermediate.locations')
    assert result.passed, str(result)


# Arguments 
args = { 
    'owner': 'Airflow', 
    'depends_on_past': False, 
    'start_date': days_ago(1), 
} 

# Dag 
dag = DAG( 
    dag_id='example_007', 
    default_args=args, 
) 

# Tasks 
preparation = PythonOperator(
    task_id='preparation',
    python_callable=preparation_wrapper_function,
    #op_kwargs={'prj': prj},
    dag=dag,
)

first_validation = PythonOperator(
    task_id='first_validation',
    python_callable=first_validation_wrapper_function,
    #op_kwargs={'prj': prj},
    dag=dag,
)

second_validation = PythonOperator(
    task_id='second_validation',
    python_callable=second_validation_wrapper_function,
    #op_kwargs={'prj': prj},
    dag=dag,
)

# Pipelines 
preparation >> first_validation >> second_validation
