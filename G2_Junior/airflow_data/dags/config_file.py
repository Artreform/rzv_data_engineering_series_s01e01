import pendulum
import pandas as pd

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pathlib import Path

INPUTS = Variable.get('load_data_conf', deserialize_json=True)
DWH_CONN_ID = 'dwh_conn'
DWH_HOOK = PostgresHook(postgres_conn_id=DWH_CONN_ID)


def get_columns(table:str, schema:str):
    root_table = INPUTS["tables"][table]
    
    columns = []
    for col in root_table["columns"]:
        column_type = root_table["columns"][col]
        columns.append(col + " " + column_type)
    
    columns_tech = []
    for col in root_table["tech_columns"]:
        column_type = root_table["tech_columns"][col]
        columns_tech.append(col + " " + column_type)
    
    columns.extend(columns_tech)

    return columns


def tables_check(table, schema):
    "Check if table exists"
    try:
        sql = f"""select 1 from {schema}.{table} limit 1"""
        DWH_HOOK.get_first(sql)
        return True
    
    except:
        return False


def create_tables(table:str, schema:str):

    if tables_check(table, schema) == True and schema == 'staging':
        sql = f"""truncate staging.{table};"""
    elif tables_check(table, schema):
        sql = None
    else:
        columns = get_columns(table, schema)
        sql = f"""create table {schema}.{table} ({", ".join(columns)});"""

    return sql

def file_route(table, etl_stage, execution_date, main_dir, format, conn_id=""):
    output_file = f"""{etl_stage}_{table}_{conn_id}_{execution_date.strftime("%Y-%m-%dT%H-%M")}.{format}"""
    output_dir = Path(main_dir + f"""/{execution_date.strftime("%Y-%m-%dT%H-%M")}""")
    output_dir.mkdir(parents=True, exist_ok=True)

    return str(Path(output_dir / output_file))











