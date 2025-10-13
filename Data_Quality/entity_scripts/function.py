# entity_scripts/function.py
import json, os, traceback
import pandas as pd
import pyodbc
import snowflake.connector
from Data_Quality.compare import Compare
# from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    df.columns = df.columns.astype(str).str.lower()
    df = df.reindex(sorted(df.columns), axis=1)
    try:
        if not df.empty: df = df.sort_values(by=df.columns.tolist(), na_position='last').reset_index(drop=True)
    except TypeError: pass
    return df

def fetch_sql_server_data(SQL_SERVER_CONFIG, query: str) -> pd.DataFrame:
    with pyodbc.connect(f"DRIVER={{{SQL_SERVER_CONFIG['driver']}}};SERVER={SQL_SERVER_CONFIG['server']};DATABASE={SQL_SERVER_CONFIG['database']};UID={SQL_SERVER_CONFIG['username']};PWD={SQL_SERVER_CONFIG['password']};TrustServerCertificate=yes;") as conn:
        return pd.read_sql_query(query, conn)

def fetch_snowflake_data(SNOWFLAKE_CONFIG, query: str) -> pd.DataFrame:
    with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetch_pandas_all()

class FunctionComparer:
    def __init__(self, config: dict):
        self.json_path = './inputs/function_input.json'
        self.function_tests = {}
        self._load_function_tests()
        if not config:
            raise ValueError("Configuration dictionary is required for database connections.")
        self.snowflake_config = config.get('SNOWFLAKE_CONFIG')
        if not self.snowflake_config:
            raise ValueError("SNOWFLAKE_CONFIG is missing in the provided configuration.")
        self.sql_server_config = config.get('SQL_SERVER_CONFIG')
        if not self.sql_server_config:
            raise ValueError("SQL_SERVER_CONFIG is missing in the provided configuration.")

    def _load_function_tests(self):
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r') as f: self.function_tests = json.load(f)
        except Exception as e: print(f"FUNC_COMPARER: LoadTests Error: {e}")

    def get_available_items(self):
        return sorted(list(self.function_tests.keys()))

    def _perform_comparison(self, func_name: str, comp_inst: Compare) -> dict:
        sf_n, sql_n = f"{func_name} (SF Test)", f"{func_name} (SQL Test)"
        try:
            queries = self.function_tests.get(func_name)
            if not queries: raise ValueError(f"Function '{func_name}' not found in JSON definitions.")
            
            sql_q = queries.get("sql_function_query")
            sf_q = queries.get("sf_function_query")

            if not sql_q or not sf_q: raise ValueError(f"Function '{func_name}' is missing a query in JSON.")

            df_sql = fetch_sql_server_data(self.sql_server_config, sql_q)
            df_sf = fetch_snowflake_data(self.snowflake_config, sf_q)

            details = comp_inst.compare_results(normalize_dataframe(df_sf.copy()), normalize_dataframe(df_sql.copy()), sf_n, sql_n, 'Function')
            is_uniform = comp_inst.is_comparison_uniform(details)
            return {"sf_name": sf_n, "sql_name": sql_n, "details": details, "is_uniform": is_uniform}
        except Exception as e:
            tb = traceback.format_exc()
            error_details = [{"Attribute":"Execution Error", "Snowflake Output": str(e), "SQL Server Output": str(e), "Comparison": "Error"},
                             {"Attribute":"Traceback", "Snowflake Output": tb, "SQL Server Output": tb, "Comparison": "Traceback"}]
            return {"sf_name": sf_n, "sql_name": sql_n, "details": error_details, "is_uniform": False}

    def compare_all_functions(self) -> list:
        res=[]
        items=self.get_available_items()
        if not items: return [{"sf_name":"N/A", "sql_name":"N/A", "details":[{"Attribute":"Status","Snowflake Output":"-","SQL Server Output":"No functions in JSON."}], "is_uniform": False}]
        comp_obj=Compare()
        for n in items: res.append(self._perform_comparison(n,comp_obj))
        if items: comp_obj.generate_comparison_html_from_structured_data("All_Functions","Function")
        return res

    def compare_specific_items(self, func_names_list: list) -> list:
        res=[]
        if not func_names_list: return [{"sf_name":"N/A", "sql_name":"N/A", "details":[{"Attribute":"Selection","Snowflake Output":"-","SQL Server Output":"No functions selected."}], "is_uniform": False}]
        avail=self.get_available_items()
        comp_obj=Compare(); pc=0
        for n in func_names_list:
            if n not in avail: res.append({"sf_name":f"SkipSF_{n}", "sql_name":f"SkipSQL_{n}", "details":[{"Attribute":"Skipped","Snowflake Output":"-","SQL Server Output":f"Function '{n}' not in JSON."}], "is_uniform": False}); continue
            res.append(self._perform_comparison(n,comp_obj)); pc+=1
        if pc>0: comp_obj.generate_comparison_html_from_structured_data(f"Selected_Functions_{pc}_items","Function")
        return res