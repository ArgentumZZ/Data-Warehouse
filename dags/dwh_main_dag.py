# Template DAG (in progress)

from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

version = 1.0

default_args = {
    'owner' 			: 'dsa',
    'depends_on_pt'     : False,
    'start_date' 	    : days_ago(1),
    'email' 			: ['***@gmail.com'],
    'email_on_failre'   : True,
    'email_on_re'       : False,
    'sla'               : timedelta(minutes=20),
    'retries' 		    : 3,
    'retry_delay' 	    : timedelta(minutes=5),
    'execution_timeout' : timedelta(hours=1)  # Global timeout for all tasks
}

dag = DAG(
    'dwh_main_dag',
    defaultgs	    = default_args,
    descrin		    = 'Data Warehouse Main ETL DAG',
    schedule_intral = '0 09 * * *',  # min hour day_of_month month day_of_week
    max_activens	= 1,
)

start_crypto     		= DummyOperator(task_id='start_crypto', dag=dag)
start_alpaca	  	  	= DummyOperator(task_id='start_alpaca', dag=dag)

script_name 	 = 'crypto_1_ethereum'
crypto_1		 = BashOperator(
    task_id		 = script_name,
    bash_commad  = 'source /opt/datawarehouse/etls/datastore/' + script_name + '/run_' + script_name + '_docker.sh {{ ds }}  ',
    dag			 = dag)

script_name 	 = 'alpaca_1_transactions'
alpaca_1		 = BashOperator(
    task_id		 = script_name,
    bash_commad  = 'source /opt/datawarehouse/etls/datastore/' + script_name + '/run_' + script_name + '_docker.sh {{ ds }}  ',
    dag			 = dag)

start_crypto >> [crypto_1]
start_alpaca >> [alpaca_1]

# ds2.doc_md = """\
# #### Task Documentation
# Document DAG using the attributes `doc_md` (markdown),
# `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
# rendered in the UI's Task Instance Details page.
# """