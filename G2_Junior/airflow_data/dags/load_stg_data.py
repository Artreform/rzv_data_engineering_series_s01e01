import pandas as pd
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timezone, timedelta
from pathlib import Path
from config_file import INPUTS, DWH_CONN_ID, DWH_HOOK, get_columns, tables_check, create_tables, file_route


default_args = {
    'depends_on_past': False,
    "start_date": datetime(2024, 2, 7, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "catchup": False,
    "tags": "staging"
}

@dag(default_args = default_args, description = 'Pipeline to load from OLTP sources to DWH(Staging)', schedule_interval='@hourly', catchup=False)
def load_staging_data():

    @task()
    def prepare_tables():
        """ Staging tables preparation"""

        for table in INPUTS["tables"]:
            sql = create_tables(table, "staging")
            if sql: DWH_HOOK.run(sql)
    
    prepare_tables_task = prepare_tables()


    @task()
    def extract_data(table, conn_id, **context):
        increment_col = INPUTS["tables"][table]["load_params"]["increment_col"]
        source_hook = PostgresHook(postgres_conn_id = conn_id)
        print(context["dag_run"].execution_date)

        try:
            sql = f"""select max({increment_col}) from core.{table};"""
            max_load_date = DWH_HOOK.get_first(sql)[0] 
        except:
            max_load_date = None
            print(f"First load of {table} table")
        print(f"Got max({increment_col}) from target {table} table: {max_load_date}")
        
        if not max_load_date:
            sql = f"""select * from {table};"""
        else:
            sql = f"""select * from {table} where {increment_col} > '{max_load_date}'::timestamp;"""
        df = source_hook.get_pandas_df(sql)
        
        execution_date = context["dag_run"].execution_date

        file_path = file_route(table, "extract", execution_date, "/tmp/airflow_staging",'csv',conn_id)

        df.to_csv(file_path, sep=';', header=True, index=False, mode='w', encoding='utf-8')
        print(f"Data extracted {df.shape[0]} rows, {df.shape[1]} columns, from source {conn_id} with {max_load_date} to {file_path}")

        return file_path


    @task()
    def transform(file_path, table, conn_id, **context):
         
         df = pd.read_csv(file_path, sep=';', header=0, index_col=None, encoding='utf-8')
         df['src_id'] = INPUTS["source_conn_ids"][conn_id]["city_name"]
         
         execution_date = context["dag_run"].execution_date
         file_path = file_route(table, "transform", execution_date, "/tmp/airflow_staging", 'csv', conn_id)
         
         df.to_csv(file_path, sep=';', header=True, index=False, mode='w', encoding='utf-8')
         print(f"Data transformed {df.shape[0]} rows, {df.shape[1]} columns, from source {conn_id} to {file_path}")
         
         return file_path
    

    @task()
    def load(file_path, table):
         df = pd.read_csv(file_path, sep=';', header=0, index_col=None, encoding='utf-8')
         
         tech_load_col = [] #видимо данная конструкция предполагает будущее добавление колонок?
         for column in INPUTS['tables'][table]['tech_load_column'].items():
              tech_load_col.append(column[0])
         
         df[tech_load_col[0]] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
         df.to_sql(table, DWH_HOOK.get_sqlalchemy_engine(), schema='staging', if_exists='append', chunksize=1000, index=False) #почему тут sqlalchemy, до этого обходились без него
         
         print(f"Data loaded {df.shape[0]} rows, {df.shape[1]} columns, into staging.{table} from {file_path}")
    
    create_schema_task = PostgresOperator(
        task_id = 'create_schema',
        postgres_conn_id = DWH_CONN_ID,
        sql = """create schema if not exists staging;"""
    )

    trigger_load_core_data = TriggerDagRunOperator(
         task_id='trigger_load_core',
         trigger_dag_id='load_core_data',
         wait_for_completion=True,
         deferrable=True,
         retries=1
    )


    @task_group()
    def etl_data():
        for conn_id in INPUTS["source_conn_ids"]:
            for table in INPUTS["source_conn_ids"][conn_id]["tables"]:
                        file_path_e = extract_data(table, conn_id)
                        file_path_t = transform(file_path_e, table, conn_id)
                        load(file_path_t, table)
    

    create_schema_task >> prepare_tables_task >> etl_data() >> trigger_load_core_data

dag = load_staging_data()

    





    






