import pandas as pd
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timezone, timedelta
from pathlib import Path
from config_file import INPUTS, DWH_CONN_ID, DWH_HOOK, get_columns, tables_check, create_tables, file_route, hashed_columns

import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.DEBUG,
    datefmt='%m/%d/%Y %I:%M:%S %p')

default_args = {
    'depends_on_past': False,
    "start_date": datetime(2024, 2, 14, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "catchup": False,
    "tags": "core"
}

def _prepare_cte_sql(df, table):
    #preparation of temporary CTE and return SQL UPDATE statement which affect rows that has been changed

    #PK columns that help us find changed rows
    pk_columns = INPUTS["tables"][table]["pk_cols_without_scd2"] 

    #Ingestion time - this value will replace "eff_to_dttm"
    tech_load_column = []
    for cols in INPUTS["tables"][table]["tech_load_column"].items():
        tech_load_column.append(cols)
    
    cols = pk_columns.copy()
    cols.append(tech_load_column[0][0])
    
    scd2_columns = []
    for columns in INPUTS["tables"][table]["load_params"]["scd2_columns"].items():
        scd2_columns.append(columns)
    
    df_copy = df[cols]
    
    # preparation of values from changed rows to use it in CTE expression (itertuples may be faster than itterrows?) Thanks to GPT for the expression:)
    values_sql = ",\n".join([
    "({})".format(", ".join([
        f"'{val}'" if not isinstance(val, int) else str(val) for val in row[1:]
    ])) for row in df_copy.itertuples()
    ])
    
    cte_name = 'temp_u'
    cte_sql = f"""
    WITH {cte_name} ({', '.join(cols)}) AS (
    VALUES
    {values_sql}
    )
    """
    #join statement to insert in SQL query (PK columns)
    merge_cols = []
    for column in pk_columns:
        merge_cols.append(f"target.{column} = {cte_name}.{column}")
    
    sql = f"""{cte_sql}
    UPDATE core.{table} AS target
    SET {scd2_columns[1][0]} = {cte_name}.{tech_load_column[0][0]}::timestamp
    FROM {cte_name}
    WHERE {' AND '.join(merge_cols)} AND target.{scd2_columns[1][0]} = '9999-12-31 23:59:59'::timestamp;
    """
    
    return sql 


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
        logging.debug(sql)
        df = DWH_HOOK.get_pandas_df(sql)
        execution_date = context['dag_run'].execution_date
        
        file_path = file_route(table, "extract", execution_date, "/tmp/airflow_staging", "csv")
        df.to_csv(file_path, sep=';', header=True, index=False, mode='w', encoding="utf-8")
        logging.info(f"Data extracted {df.shape[0]} rows, {df.shape[1]} columns from staging.{table}, saved to {file_path} ")
        
        return file_path

    @task()
    def transform(file_path, table, **context):
        
        df = pd.read_csv(file_path, sep=';', header=0, index_col=None, encoding="utf-8")
        increment_col = INPUTS["tables"][table]["load_params"]["increment_col"]

        scd_columns = []
        for cols in INPUTS["tables"][table]["load_params"]['scd2_columns'].items():
            scd_columns.append(cols)
        

        sql = f"""select max({increment_col}) from core.{table};"""
        logging.debug(sql)
        max_load_date = DWH_HOOK.get_first(sql)[0]
        logging.info(f"Max loaded date from core.{table} is {max_load_date}")
        file_path_u = None
        df_insert = df.copy(deep=True)

        if max_load_date:
            df_insert = df_insert[pd.to_datetime(df_insert[increment_col]) > max_load_date]
        
        #INSERT part
        df_insert[scd_columns[0][0]] = df_insert[increment_col] #effective date from
        df_insert[scd_columns[1][0]] = datetime(9999, 12, 31, 23, 59, 59) #effective date to

        execution_date = context["dag_run"].execution_date
        
        file_path_i = file_route(table, "transform_insert", execution_date, "/tmp/airflow_staging", "csv")
        df_insert.to_csv(file_path_i, sep=';', header=True, index=False, mode='w', encoding="utf-8")
        logging.info(f"Data transformed for insert {df_insert.shape[0]} rows, {df_insert.shape[1]} columns from staging.{table}, saved to {file_path} ")

        if max_load_date:
            df_update_stg = df.copy(deep=True)
            df_update_stg = df_update_stg[pd.to_datetime(df_update_stg[increment_col]) <= max_load_date]

            #UPDATE part: for updating rows we should check if rows changed or not (hash func present in config file)
            # 1. we hash extracted data which load datetime < max load date
        
            df_update_hashed = hashed_columns(df_update_stg, table)

            # 2. we hash data that are already downloaded in core надо проверить этот sql запрос, пока не совсем понятно
            sql = f"""select * from core.{table} 
            where {scd_columns[1][0]} = '9999-12-31 23:59:59';"""
            logging.debug(sql)
            df_update_core = DWH_HOOK.get_pandas_df(sql)
            df_update_core_hashed = hashed_columns(df_update_core, table)

            # 3. Comparison of 2 datasets, leave only difference in a rows
            pk_columns = INPUTS["tables"][table]["pk_cols_without_scd2"]
            df_merged = pd.merge(df_update_hashed, df_update_core_hashed, how='inner', on=pk_columns)
            df_diff_val = df_merged[df_merged['hashed_x']!= df_merged['hashed_y']]
            df_diff_val = df_diff_val.drop(columns=["hashed_x", "hashed_y"])
            
            #4. If there are changes, then we save that rows from staging and update scd fields
            # P.S. Aitflow ругался на shape если он пустой, пришлось поставить заглушку is not None
            if df_diff_val is not None and df_diff_val.shape[0]>0:
                df_update = pd.merge(df_diff_val, df_update_stg, how='inner', on=pk_columns)
                df_update["ingested_at"] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                df_update[scd_columns[0][0]] = df_update["ingested_at"]
                df_update[scd_columns[1][0]] = datetime(9999, 12, 31, 23, 59, 59)
                file_path_u = file_route(table, "transform_update", execution_date, "/tmp/airflow_staging", "csv")
                df_update.to_csv(file_path_u, sep=';', header=True, index=False, mode='w', encoding="utf-8")
                logging.info(f"Data transformed for update {df_update.shape[0]} rows, {df_update.shape[1]} columns from staging.{table}, saved to {file_path} ")
            else:
                logging.info("There is nothing to update this time")
        
        return(file_path_i, file_path_u)
    
    
    @task()
    def load(file_path, table):

        tech_load_col = [] 
        for column in INPUTS['tables'][table]['tech_load_column'].items():
            tech_load_col.append(column[0])
        
        #UPDATE part
        if file_path[1]:
            df_update=pd.read_csv(file_path[1], sep=';', header=0, index_col=None, encoding="utf-8")

            #update "eff_dt_to" in rows that has been changed
            sql = _prepare_cte_sql(df_update, table)
            logging.debug(sql)
            DWH_HOOK.run(sql)
            logging.info(f"Old rows has been updated {df_update.shape[0]} rows into core.{table} from {file_path[1]}")

            #update rows (new version)
            df_update.to_sql(table,
                    DWH_HOOK.get_sqlalchemy_engine(),
                    schema='core',
                    chunksize=1000,
                    if_exists='append',
                    index=False
                    )
            logging.info(f"New versions of rows has been added {df_update.shape[0]} rows into core.{table} from {file_path[1]}")

        #INSERT part
        if file_path[0]:
            df_insert=pd.read_csv(file_path[0], sep=';', header=0, index_col=None, encoding="utf-8")
            df_insert[tech_load_col[0]] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            df_insert.to_sql(table,
                    DWH_HOOK.get_sqlalchemy_engine(),
                    schema='core',
                    chunksize=1000,
                    if_exists='append',
                    index=False
                    )
            logging.info(f"Data loaded {df_insert.shape[0]} rows into core.{table} from {file_path[0]}")


    for table in INPUTS["tables"]:
        @task_group(group_id=table)
        def etl_data():
            file_path_e = extract(table)
            file_path_t = transform(file_path_e, table)
            load(file_path_t, table)
            
        create_schema_task >> prepare_tables_task >> etl_data()

dag = load_core_data()