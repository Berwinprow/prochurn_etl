import json
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook


# --------------------------------------------------------------------------------------
# CREATING THE SCHEMAS IF NOT EXISTS  
# --------------------------------------------------------------------------------------
def ensure_all_schemas(conn_id: str, json_path: str) -> None:
    with open(json_path, "r") as f:
        cfg = json.load(f)

    hook = PostgresHook(postgres_conn_id = conn_id)
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        for _, schema_name in cfg["schemas"].items():
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'))

# --------------------------------------------------------------------------------------
# EXTRACTING THE META LOGS TABLE DETAILS FROM JSON FILE AND STORING IT IN PYTHON DICT  
# --------------------------------------------------------------------------------------
def get_logtable_details_from_json(conn_id: str, json_path: str) -> None:


    with open(json_path, "r") as f:
        config = json.load(f)
        print("Json loaded into Python Dict")

        log_schema = config["schemas"]["log"]
        tables = config[log_schema]

        hook = PostgresHook(postgres_conn_id = conn_id)
        engine = hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{log_schema}";'))
            print(f"{log_schema} created")

            for table_name,columns in tables.items():
                column_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in columns.items()])
                ddl = f'CREATE TABLE IF NOT EXISTS "{log_schema}"."{table_name}" ({column_defs});'
                conn.execute(text(ddl))
                print(f"{log_schema}.{table_name} created")
        print("done")

# --------------------------------------------------------------------------------------
# EXTRACTING THE SCHEMA DETAILS FROM JSON FILE AND STORING IT IN PYTHON DICT  
# --------------------------------------------------------------------------------------

def get_schema(name: str, json_path: str) -> str:
    with open(json_path, "r")as f:
        data = json.load(f)
    return data["schemas"][name]

# --------------------------------------------------------------------------------------
# EXTRACTING THE METALOG INFO  FROM JSON FILE AND STORING IT IN PYTHON DICT  
# --------------------------------------------------------------------------------------

def get_log_tables(name: str, json_path: str) -> str:
    with open(json_path, "r") as f:
        data = json.load(f)
    return data["logtables"][name]

# --------------------------------------------------------------------------------------
# EXTRACTING THE COLUMN MAPPING FROM JSON FILE AND STORING IT IN PYTHON DICT  
# --------------------------------------------------------------------------------------

def get_column_mapping(file_type: str, json_path: str) -> dict:
    with open(json_path, "r") as f:
        data = json.load(f)
    return data[file_type]

