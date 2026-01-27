# import libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

version = 1.0

default_args = {
    'owner' 			: 'Mihail',
    'start_date' 	    : datetime(2026, 1, 1),
    'email' 			: ['liahim13@gmail.com'],
    'email_on_failre'   : True,
    'email_on_re'       : False,
    'sla'               : timedelta(minutes=20),
    'retries' 		    : 3,
    'retry_delay' 	    : timedelta(minutes=1),
    'execution_timeout' : timedelta(hours=1)  # Global timeout for all tasks
}

dag = DAG(
    'dwh_main_dag',
    default_args	  = default_args,
    description       = 'Data Warehouse Main ETL DAG',
    schedule_interval = '*/1 * * * *',  # min hour day_of_month month day_of_week
    max_active_runs	  = 1,
    catchup           = False
)

start_crypto     		= EmptyOperator(task_id='start_crypto', dag=dag)
# start_alpaca	  	  	= DummyOperator(task_id='start_alpaca', dag=dag)

script_name 	 = 'financial_data_1_ethereum'
crypto_1		 = BashOperator(
    task_id		 = script_name,
    bash_command = 'source /opt/airflow/etls/datastore/' + script_name + '/docker' + '/run_' + script_name + '_docker.sh {{ ds }}',
    dag			 = dag)

# script_name 	 = 'alpaca_1_transactions'
# alpaca_1		 = BashOperator(
#    task_id		 = script_name,
#    bash_commad  = 'source /opt/airflow/etls/datastore/' + script_name + '/run_' + script_name + '_docker.sh {{ ds }}  ',
#    dag			 = dag)

start_crypto >> [crypto_1]
# start_alpaca >> [alpaca_1]

# ds2.doc_md = """\
# #### Task Documentation
# Document DAG using the attributes `doc_md` (markdown),
# `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
# rendered in the UI's Task Instance Details page.
# """