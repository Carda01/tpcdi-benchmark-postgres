import os
import airflow
from datetime import timedelta, datetime
from airflow import DAG
# from airflow.operators.postgres_operator import PostgresOperator #deprecated
from airflow.models.connection import Connection
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator


SF=INPUT_SF

# Default arguments for dag
default_args = {
    'owner': 'airflow',
    # 'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 30)
}


# Create dag
dag_incr = DAG(
    dag_id = "incremental_second_upload_sf_"+str(SF),
    default_args = default_args,	
    #dagrun_timeout = timedelta(minutes=60),
    #description = 'TPC-DI project',
    #schedule = None,
    schedule_interval = None,
    catchup = False
)

"""
create_schema_staging = PostgresOperator(
    task_id = "create_schema_staging",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/create_schema_staging.sql",
    dag = dag_incr
)

truncate_staging = PostgresOperator(
    task_id = "truncate_staging",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/truncate_staging.sql",
    dag = dag_incr
)

load_staging = PostgresOperator(
    task_id = "load_staging",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/load_staging.sql",
    dag = dag_incr
)

tl_master_dimtrade = PostgresOperator(
    task_id = "tl_master_dimtrade",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_dimtrade.sql",
    dag = dag_incr
)

tl_master_dimaccount = PostgresOperator(
    task_id = "tl_master_dimaccount",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_dimaccount.sql",
    dag = dag_incr
)

tl_master_dimcustomer = PostgresOperator(
    task_id = "tl_master_dimcustomer",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_dimcustomer.sql",
    dag = dag_incr
)

tl_master_factcashbalances = PostgresOperator(
    task_id = "tl_master_factcashbalances",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factcashbalances.sql",
    dag = dag_incr
)

tl_master_factholdings = PostgresOperator(
    task_id = "tl_master_factholdings",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factholdings.sql",
    dag = dag_incr
)

tl_master_factmarkethistory = PostgresOperator(
    task_id = "tl_master_factmarkethistory",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factmarkethistory.sql",
    dag = dag_incr
)

tl_master_factwatches = PostgresOperator(
    task_id = "tl_master_factwatches",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factwatches.sql",
    dag = dag_incr
)

prospect = PostgresOperator(
    task_id = "prospect",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/prospect.sql",
    dag = dag_incr
)

update_prospect = PostgresOperator(
    task_id = "update_prospect",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/update_prospect.sql",
    dag = dag_incr
)
"""
"""finish_incremental_update_b2 = BashOperator(
    task_id = "finish_incremental_update_b2",
    bash_command = "date -u --rfc-3339='seconds' >> /home/workspace/times.txt",
    dag = dag_incr
)"""

truncate_staging_b3 = PostgresOperator(
    task_id = "truncate_staging_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/truncate_staging.sql",
    dag = dag_incr
)

load_staging_b3 = PostgresOperator(
    task_id = "load_staging_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/load_staging_b3.sql",
    dag = dag_incr
)

tl_master_dimtrade_b3 = PostgresOperator(
    task_id = "tl_master_dimtrade_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_dimtrade_b3.sql",
    dag = dag_incr
)

tl_master_dimaccount_b3 = PostgresOperator(
    task_id = "tl_master_dimaccount_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_dimaccount_b3.sql",
    dag = dag_incr
)

tl_master_dimcustomer_b3 = PostgresOperator(
    task_id = "tl_master_dimcustomer_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_dimcustomer_b3.sql",
    dag = dag_incr
)

tl_master_factcashbalances_b3 = PostgresOperator(
    task_id = "tl_master_factcashbalances_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factcashbalances_b3.sql",
    dag = dag_incr
)

tl_master_factholdings_b3 = PostgresOperator(
    task_id = "tl_master_factholdings_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factholdings_b3.sql",
    dag = dag_incr
)

tl_master_factmarkethistory_b3 = PostgresOperator(
    task_id = "tl_master_factmarkethistory_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factmarkethistory_b3.sql",
    dag = dag_incr
)

tl_master_factwatches_b3 = PostgresOperator(
    task_id = "tl_master_factwatches_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/tl_master_factwatches_b3.sql",
    dag = dag_incr
)

prospect_b3 = PostgresOperator(
    task_id = "prospect_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/prospect_b3.sql",
    dag = dag_incr
)

update_prospect_b3 = PostgresOperator(
    task_id = "update_prospect_b3",
    postgres_conn_id = "postgres_"+str(SF),
    sql = "incremental_update/update_prospect.sql",
    dag = dag_incr
)

"""finish_incremental_update_b3 = BashOperator(
    task_id = "finish_incremental_update_b3",
    bash_command = "date -u --rfc-3339='seconds' >> /home/workspace/times.txt && echo 'Done!' >> /home/workspace/times.txt",
    dag = dag_incr
)"""

"""
create_schema_staging >> truncate_staging
truncate_staging >> load_staging
load_staging >> tl_master_dimcustomer >> tl_master_dimaccount >> tl_master_dimtrade
tl_master_dimtrade >> tl_master_factholdings #>> finish_incremental_update_b2
tl_master_dimtrade >> tl_master_factmarkethistory #>> finish_incremental_update_b2
tl_master_dimtrade >> tl_master_factwatches #>> finish_incremental_update_b2
tl_master_dimtrade >> tl_master_factcashbalances #>> finish_incremental_update_b2
load_staging >> prospect >> update_prospect #>> finish_incremental_update_b2
"""
#finish_incremental_update_b2 >> truncate_staging_b3

truncate_staging_b3 >> load_staging_b3
load_staging_b3 >> tl_master_dimcustomer_b3 >> tl_master_dimaccount_b3 >> tl_master_dimtrade_b3
tl_master_dimtrade_b3 >> tl_master_factholdings_b3 #>> finish_incremental_update_b3
tl_master_dimtrade_b3 >> tl_master_factmarkethistory_b3 #>> finish_incremental_update_b3
tl_master_dimtrade_b3 >> tl_master_factwatches_b3 #>> finish_incremental_update_b3
tl_master_dimtrade_b3 >> tl_master_factcashbalances_b3 #>> finish_incremental_update_b3

load_staging_b3 >> prospect_b3 >> update_prospect_b3 #>> finish_incremental_update_b3
