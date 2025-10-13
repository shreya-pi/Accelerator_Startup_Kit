# entity_scripts/procedures.py (FINAL - Reverts to Persistent Connection)
import json, os, traceback
import pandas as pd
import pyodbc
import snowflake.connector
from Data_Quality.compare import Compare
# from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG

class ProcedureComparer:
    @staticmethod
    def normalize_dataframe_static(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty: return pd.DataFrame()
        df.columns = df.columns.astype(str).str.lower()
        df = df.reindex(sorted(df.columns), axis=1)
        try:
            if not df.empty:
                df = df.sort_values(by=df.columns.tolist(), na_position='last').reset_index(drop=True)
        except TypeError: pass
        return df
    normalize_dataframe = lambda self, df: self.__class__.normalize_dataframe_static(df)

    def __init__(self, config: dict):
        # self.snowflake_config = SNOWFLAKE_CONFIG
        # self.sql_server_config = SQL_SERVER_CONFIG
        self.snowflake_config = config.get('SNOWFLAKE_CONFIG')
        self.sql_server_config = config.get('SQL_SERVER_CONFIG')
        if not self.snowflake_config or not self.sql_server_config:
            raise ValueError("Both SNOWFLAKE_CONFIG and SQL_SERVER_CONFIG must be provided in the configuration dictionary.")
        self.json_path = './inputs/procedure_input.json'
        self.sf_conn, self.sql_conn = None, None
        # Create the connections once during initialization
        self._connect_databases()
        self.procedure_tests = {}
        self._load_procedure_tests()

    def _connect_databases(self):
        try:
            self.sf_conn = snowflake.connector.connect(**self.snowflake_config)
        except Exception as e: raise ConnectionError(f"SF Conn ProcComparer: {e}")
        try:
            driver = self.sql_server_config['driver']
            if '{' not in driver: driver = f"{{{driver}}}"
            conn_str=f"DRIVER={driver};SERVER={self.sql_server_config['server']};DATABASE={self.sql_server_config['database']};UID={self.sql_server_config['username']};PWD={self.sql_server_config['password']};TrustServerCertificate=yes;"
            self.sql_conn = pyodbc.connect(conn_str)
        except Exception as e: raise ConnectionError(f"SQL Conn ProcComparer: {e}")

    def _close_connections(self):
        if self.sf_conn and not self.sf_conn.is_closed(): self.sf_conn.close()
        if self.sql_conn: self.sql_conn.close()
    def __del__(self): self._close_connections()

    def _load_procedure_tests(self):
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r') as f: self.procedure_tests = json.load(f)
        except Exception as e: print(f"PROC_COMPARER: LoadTests Error: {e}")

    def get_available_items(self):
        return sorted(list(self.procedure_tests.keys()))

    def _perform_comparison(self, proc_name: str, comp_inst: Compare) -> dict:
        sf_n, sql_n = f"{proc_name} (SF Test)", f"{proc_name} (SQL Test)"
        try:
            queries = self.procedure_tests.get(proc_name)
            if not queries: raise ValueError(f"Procedure '{proc_name}' not found in JSON definitions.")
            
            sql_verify_q = queries.get("sql_verification_query")
            sf_verify_q = queries.get("sf_verification_query")

            if not sql_verify_q or not sf_verify_q: raise ValueError(f"Procedure '{proc_name}' is missing a verification query in JSON.")

            # Use the persistent connection objects
            df_sql = pd.read_sql_query(sql_verify_q, self.sql_conn)
            
            # Use a fresh cursor from the persistent connection
            with self.sf_conn.cursor() as sf_cursor:
                sf_cursor.execute(sf_verify_q)
                df_sf = sf_cursor.fetch_pandas_all()

            details = comp_inst.compare_results(self.normalize_dataframe(df_sf.copy()), self.normalize_dataframe(df_sql.copy()), sf_n, sql_n, 'Procedure')
            is_uniform = comp_inst.is_comparison_uniform(details)
            return {"sf_name": sf_n, "sql_name": sql_n, "details": details, "is_uniform": is_uniform}
        except Exception as e:
            tb = traceback.format_exc()
            error_details = [{"Attribute":"Execution Error", "Snowflake Output": str(e), "SQL Server Output": str(e), "Comparison": "Error"},
                             {"Attribute":"Traceback", "Snowflake Output": tb, "SQL Server Output": tb, "Comparison": "Traceback"}]
            return {"sf_name": sf_n, "sql_name": sql_n, "details": error_details, "is_uniform": False}

    # compare_all_procedures and compare_specific_items methods are unchanged
    def compare_all_procedures(self) -> list:
        res=[]
        items=self.get_available_items()
        if not items: return [{"sf_name":"N/A", "sql_name":"N/A", "details":[{"Attribute":"Status","Snowflake Output":"-","SQL Server Output":"No procedures in JSON."}], "is_uniform": False}]
        comp_obj=Compare()
        for n in items: res.append(self._perform_comparison(n,comp_obj))
        if items: comp_obj.generate_comparison_html_from_structured_data("All_Procedures","Procedure")
        return res

    def compare_specific_items(self, proc_names_list: list) -> list:
        res=[]
        if not proc_names_list: return [{"sf_name":"N/A", "sql_name":"N/A", "details":[{"Attribute":"Selection","Snowflake Output":"-","SQL Server Output":"No procs selected."}], "is_uniform": False}]
        avail=self.get_available_items()
        comp_obj=Compare(); pc=0
        for n in proc_names_list:
            if n not in avail: res.append({"sf_name":f"SkipSF_{n}", "sql_name":f"SkipSQL_{n}", "details":[{"Attribute":"Skipped","Snowflake Output":"-","SQL Server Output":f"Proc '{n}' not in JSON."}], "is_uniform": False}); continue
            res.append(self._perform_comparison(n,comp_obj)); pc+=1
        if pc>0: comp_obj.generate_comparison_html_from_structured_data(f"Selected_Procedures_{pc}_items","Procedure")
        return res