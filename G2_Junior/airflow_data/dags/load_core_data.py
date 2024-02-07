import pandas as pd
import pendulum

from airflow.decorators import dag, task, task_group
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
    "tags": "core"
}

@dag(default_args=default_args, description='Pipeline to load from Staging to Core', schedule_interval=None, catchup=False)
def load_core_data():

    @task()
    def prepare_tables():
        for table in INPUTS["tables"]:
            sql = create_tables(table, 'core')
            if sql: DWH_HOOK.run(sql)
    
    prepare_tables_task = prepare_tables()

    create_schema_task = PostgresOperator(
        task_id='prepare_schema',
        postgres_conn_id=DWH_CONN_ID,
        sql="""create schema if not exists core;"""
    )

    @task()
    def extract(table, **context):
        sql = f"""select * from staging.{table};"""
        df = DWH_HOOK.get_pandas_df(sql)
        execution_date = context['dag_run'].execution_date
        
        file_path = file_route(table, "extract", execution_date, "/tmp/airflow_staging", "csv")
        df.to_csv(file_path, sep=';', header=True, index=False, mode='w', encoding="utf-8")
        print(f"Data extracted {df.shape[0]} rows, {df.shape[1]} columns from staging.{table}, saved to {file_path} ")
        
        return file_path

    @task()
    def transform(file_path, table, **context):
        
        df = pd.read_csv(file_path, sep=';', header=0, index_col=None, encoding="utf-8")
        increment_col = INPUTS["tables"][table]["load_params"]["increment_col"]
        sql = f"""select max({increment_col}) from core.{table};"""
        max_load_date = DWH_HOOK.get_first(sql)[0]
        print(f"Max loaded date from core.{table} is {max_load_date}")
        
        df_insert = df.copy(deep=True)

        if max_load_date:
            df_insert = df_insert[pd.to_datetime(df_insert[increment_col]) > max_load_date]
        
        execution_date = context["dag_run"].execution_date
        
        file_path_i = file_route(table, "transform_insert", execution_date, "/tmp/airflow_staging", "csv")
        df_insert.to_csv(file_path_i, sep=';', header=True, index=False, mode='w', encoding="utf-8")
        print(f"Data transformed for insert {df_insert.shape[0]} rows, {df_insert.shape[1]} columns from staging.{table}, saved to {file_path} ")

        return(file_path_i)
    
    @task()
    def load(file_path_i, table):

        tech_load_col = [] #видимо данная конструкция предполагает будущее добавление колонок?
        for column in INPUTS['tables'][table]['tech_load_column'].items():
            tech_load_col.append(column[0])
        
        df_insert=pd.read_csv(file_path_i, sep=';', header=0, index_col=None, encoding="utf-8")
        df_insert[tech_load_col[0]] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        df_insert.to_sql(table,
                  DWH_HOOK.get_sqlalchemy_engine(),
                  schema='core',
                  chunksize=1000,
                  if_exists='append',
                  index=False
                  )
        print(f"Data loaded {df_insert.shape[0]} rows into core.{table} from {file_path_i}")
    


    @task_group()
    def etl_data():
        for table in INPUTS["tables"]:
            file_path_e = extract(table)
            file_path_t = transform(file_path_e, table)
            load(file_path_t, table)


    create_schema_task >> prepare_tables_task >> etl_data()

dag = load_core_data()
    