import os
import airflow
from datetime import timedelta, datetime
from airflow import DAG
# from airflow.operators.postgres_operator import PostgresOperator #deprecated
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task
from customermgmt_conversion import customermgmt_convert

SF=INPUT_SF


# Default arguments for dag
default_args = {
    'owner': 'airflow',
    # 'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 30)
}

# Create dag
dag_psql = DAG(
    dag_id = f"dw_sf_{SF}",
    default_args = default_args,	
    tags = ["tpcdi"],
    description = 'Full Historical Load',
    #dagrun_timeout = timedelta(minutes=60),
    #schedule = None,
    schedule_interval = None,
    # catchup = False
)

# Task1 - Create processing schema
create_processing_schema = PostgresOperator(
    task_id = "create_processing_schema",
    postgres_conn_id = f"postgres_{SF}",
    sql = "create_processing_schema.sql",
	dag = dag_psql
)

# Task2 - Load txt and csv sources to processing
load_txt_csv_sources_to_processing = PostgresOperator(
    task_id = "load_txt_csv_sources_to_processing",
    postgres_conn_id = f"postgres_{SF}",
    sql = "processing_data_commands.sql",
	dag = dag_psql
)

# Task3 - Load finwire source to processing
load_finwire_to_processing = PostgresOperator(
    task_id = "load_finwire_to_processing",
    postgres_conn_id = f"postgres_{SF}",
    sql = "processing_finwire_load1.sql",
	dag = dag_psql
)

# Task4 - Parse finwire and load to seperate tables
parse_finwire = PostgresOperator(
    task_id = "parse_finwire",
    postgres_conn_id = f"postgres_{SF}",
    sql = "load_processing_finwire_db.sql",
	dag = dag_psql
)

# Task5 - Convert customer management source from xml to csv
convert_customermgmt_xml_to_csv = PythonOperator(
    task_id = "convert_customermgmt_xml_to_csv",
    python_callable = customermgmt_convert,
    op_kwargs = {'SF': SF},
    dag = dag_psql
)

# Task6 - Load customer management source to processing
load_customer_mgmt_to_processing = PostgresOperator(
    task_id = "load_customer_mgmt_to_processing",
    postgres_conn_id = f"postgres_{SF}",
    sql = "load_processing_customermgmt_db.sql",
	dag = dag_psql
)

# Task7 - Create master schema
create_master_schema = PostgresOperator(
    task_id = "create_master_schema",
    postgres_conn_id = f"postgres_{SF}",
    sql = "create_master_schema.sql",
	dag = dag_psql
)

# Task8 - Direct load those tables which do not require any transformations
load_directly_master_tables = PostgresOperator(
    task_id = "load_directly_master_tables",
    postgres_conn_id = f"postgres_{SF}",
    sql = "from_stage_to_master.sql",
	dag = dag_psql
)

# Task9 - Transform & load master.dimcompany
transform_load_master_dimcompany = PostgresOperator(
    task_id = "transform_load_master_dimcompany",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/1_transform_load_master_dimcompany.sql",
	dag = dag_psql
)

# Task10 - Load master.dimessages with alert from master.dimcompany
load_master_dimessages_dimcompany = PostgresOperator(
    task_id = "load_master_dimessages_dimcompany",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/2_load_master_dimessages_dimcompany.sql",
	dag = dag_psql
)

# Task11 - Transform & load master.dimbroker
transform_load_master_dimbroker = PostgresOperator(
    task_id = "transform_load_master_dimbroker",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/3_transform_load_master_dimbroker.sql",
	dag = dag_psql
)

# Task12 - Transform & load master.prospect
transform_load_master_prospect = PostgresOperator(
    task_id = "transform_load_master_prospect",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/4_transform_load_master_prospect.sql",
	dag = dag_psql
)

# Task13 - Transform & load master.dimcustomer
transform_load_master_dimcustomer = PostgresOperator(
    task_id = "transform_load_master_dimcustomer",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/5_transform_load_master_dimcustomer.sql",
	dag = dag_psql
)

# Task14 - Load master.dimessages with alert from master.dimcustomer
load_master_dimessages_dimcustomer = PostgresOperator(
    task_id = "load_master_dimessages_dimcustomer",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/6_load_master_dimessages_dimcustomer.sql",
	dag = dag_psql
)

# Task15 - Update master.prospect
update_master_prospect = PostgresOperator(
    task_id = "update_master_prospect",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/7_update_master_prospect.sql",
	dag = dag_psql
)

# Task16 - Transform & load master.dimaccount
transform_load_master_dimaccount = PostgresOperator(
    task_id = "transform_load_master_dimaccount",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/8_transform_load_master_dimaccount.sql",
	dag = dag_psql
)

# Task17 - Transform & load master.dimsecurity
transform_load_master_dimsecurity = PostgresOperator(
    task_id = "transform_load_master_dimsecurity",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/9_transform_load_master_dimsecurity.sql",
	dag = dag_psql
)

# Task18 - Transform & load master.dimtrade
transform_load_master_dimtrade = PostgresOperator(
    task_id = "transform_load_master_dimtrade",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/10_transform_load_master_dimtrade.sql",
	dag = dag_psql
)

# Task19 - Load master.dimessages with alert from master.dimtrade
load_master_dimessages_dimtrade = PostgresOperator(
    task_id = "load_master_dimessages_dimtrade",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/11_load_master_dimessages_dimtrade.sql",
	dag = dag_psql
)

# Task20 - Transform & load master.financial
transform_load_master_financial = PostgresOperator(
    task_id = "transform_load_master_financial",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/12_transform_load_master_financial.sql",
	dag = dag_psql
)

# Task21 - Transform & load master.factcashbalances
transform_load_master_factcashbalances = PostgresOperator(
    task_id = "transform_load_master_factcashbalances",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/13_transform_load_master_factcashbalances.sql",
	dag = dag_psql
)

# Task22 - Transform & load master.factholdings
transform_load_master_factholdings = PostgresOperator(
    task_id = "transform_load_master_factholdings",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/14_transform_load_master_factholdings.sql",
	dag = dag_psql
)

# Task23 - Transform & load master.factwatches
transform_load_master_factwatches = PostgresOperator(
    task_id = "transform_load_master_factwatches",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/15_transform_load_master_factwatches.sql",
	dag = dag_psql
)

# Task24 - Transform & load master.factmarkethistory
transform_load_master_factmarkethistory = PostgresOperator(
    task_id = "transform_load_master_factmarkethistory",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/16_transform_load_master_factmarkethistory.sql",
	dag = dag_psql
)

# Task25 - Load master.dimessages with alert from master.factmarkethistory
load_master_dimessages_factmarkethistory = PostgresOperator(
    task_id = "load_master_dimessages_factmarkethistory",
    postgres_conn_id = f"postgres_{SF}",
    sql = "/transformations/17_load_master_dimessages_factmarkethistory.sql",
	dag = dag_psql
)


# Task Dependencies

# Staging schema dependency
create_processing_schema >> load_txt_csv_sources_to_processing
create_processing_schema >> load_finwire_to_processing >> parse_finwire
create_processing_schema >> convert_customermgmt_xml_to_csv >> load_customer_mgmt_to_processing

# Master schema dependency
load_txt_csv_sources_to_processing >> create_master_schema
parse_finwire >> create_master_schema
load_customer_mgmt_to_processing >> create_master_schema

# Transformation/Loading to master dependency
create_master_schema >> load_directly_master_tables
load_directly_master_tables >> transform_load_master_dimcompany
transform_load_master_dimcompany >> load_master_dimessages_dimcompany
load_directly_master_tables >> transform_load_master_dimbroker
load_directly_master_tables >> transform_load_master_prospect
load_directly_master_tables >> transform_load_master_dimcustomer
load_directly_master_tables >> transform_load_master_dimsecurity
transform_load_master_prospect >> transform_load_master_dimcustomer
transform_load_master_dimcustomer >> load_master_dimessages_dimcustomer
transform_load_master_dimcustomer >> update_master_prospect
transform_load_master_dimbroker >> transform_load_master_dimaccount
transform_load_master_dimcustomer >> transform_load_master_dimaccount
transform_load_master_dimcompany >> transform_load_master_dimsecurity
transform_load_master_dimaccount >> transform_load_master_dimtrade
load_directly_master_tables >> transform_load_master_dimtrade
load_directly_master_tables >> transform_load_master_dimtrade
transform_load_master_dimsecurity >> transform_load_master_dimtrade
load_master_dimessages_factmarkethistory >> transform_load_master_dimtrade
transform_load_master_dimtrade >> load_master_dimessages_dimtrade
transform_load_master_dimcompany >> transform_load_master_financial
transform_load_master_dimaccount >> transform_load_master_factcashbalances
load_directly_master_tables >> transform_load_master_factcashbalances
transform_load_master_dimtrade >> transform_load_master_factholdings
transform_load_master_dimcustomer >> transform_load_master_factwatches
transform_load_master_dimsecurity >> transform_load_master_factwatches
load_directly_master_tables >> transform_load_master_factwatches
load_directly_master_tables >> transform_load_master_factmarkethistory
transform_load_master_financial >> transform_load_master_factmarkethistory
transform_load_master_dimcompany >> transform_load_master_factmarkethistory
transform_load_master_dimsecurity >> transform_load_master_factmarkethistory
transform_load_master_factmarkethistory >> load_master_dimessages_factmarkethistory
