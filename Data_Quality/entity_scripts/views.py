# entity_scripts/views.py
from Data_Quality.compare import Compare
# from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG
import pandas as pd
import pyodbc
import snowflake.connector

class ViewComparer:
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
        self.sf_conn, self.sql_conn = None, None
        self._connect_databases()
        self.sql_map, self.sf_map, self.common_normalized_views = {}, {}, []
        self._populate_view_maps()

    def _connect_databases(self):
        try:
            self.sf_conn = snowflake.connector.connect(**self.snowflake_config)
        except Exception as e: raise ConnectionError(f"SF Conn ViewComparer: {e}")
        try:
            driver = self.sql_server_config['driver']
            if '{' not in driver: driver = f"{{{driver}}}"
            conn_str=f"DRIVER={driver};SERVER={self.sql_server_config['server']};DATABASE={self.sql_server_config['database']};UID={self.sql_server_config['username']};PWD={self.sql_server_config['password']};TrustServerCertificate=yes;"
            self.sql_conn = pyodbc.connect(conn_str)
        except Exception as e: raise ConnectionError(f"SQL Conn ViewComparer: {e}")

    def _close_connections(self):
        if self.sf_conn and not self.sf_conn.is_closed(): self.sf_conn.close()
        if self.sql_conn: self.sql_conn.close()
    def __del__(self): self._close_connections()

    def _get_snowflake_view_names_raw(self):
        db_cfg = self.snowflake_config.get('database')
        schema_cfg = self.snowflake_config.get('schema')
        if not db_cfg or not schema_cfg: raise ValueError("Database and schema must be defined in SNOWFLAKE_CONFIG")
        query = f'SHOW VIEWS IN SCHEMA "{db_cfg}"."{schema_cfg}"'
        with self.sf_conn.cursor() as cursor:
            cursor.execute(query)
            return [f'"{schema_cfg}"."{row[1]}"' for row in cursor.fetchall()]

    def _get_sqlserver_view_names_raw(self):
        query = "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS"
        with self.sql_conn.cursor() as cursor:
            cursor.execute(query)
            return [r[0] for r in cursor.fetchall()]

    def normalize_name_internal(self, full_name: str) -> str:
        if not full_name: return ""
        return full_name.replace('"', '').split('.')[-1].lower()

    def _populate_view_maps(self):
        try:
            sql_views_raw = self._get_sqlserver_view_names_raw()
            sf_views_raw = self._get_snowflake_view_names_raw()
            self.sql_map={self.normalize_name_internal(v):v for v in sql_views_raw}
            self.sf_map={self.normalize_name_internal(v):v for v in sf_views_raw}
            common_keys = set(self.sql_map.keys()) & set(self.sf_map.keys())
            self.common_normalized_views = sorted(list(common_keys))
        except Exception as e:
            print(f"VIEW_COMPARER: PopulateMaps Error: {e}")
            self.sql_map,self.sf_map,self.common_normalized_views = {},{},[]

    def get_available_items(self):
        return self.common_normalized_views

    def fetch_view_data(self, full_name: str, platform: str):
        q = f"SELECT * FROM {full_name}"
        try:
            if platform=='sqlserver': return pd.read_sql_query(q, self.sql_conn)
            elif platform=='snowflake':
                with self.sf_conn.cursor() as cursor:
                    cursor.execute(q)
                    return cursor.fetch_pandas_all()
        except Exception as e: print(f"VIEW_COMPARER: FetchData Error for {full_name} ({platform}): {e}"); return pd.DataFrame()

    def _perform_comparison(self, norm_name: str, comp_inst: Compare) -> dict:
        sf_n, sql_n = self.sf_map.get(norm_name), self.sql_map.get(norm_name)
        if not sf_n or not sql_n:
             return {"sf_name": f"UnkSF_{norm_name}", "sql_name": f"UnkSQL_{norm_name}", "details": [{"Attribute":"Setup Error","Snowflake Output":"-","SQL Server Output":f"Orig name for '{norm_name}' not in maps."}], "is_uniform": False}
        try:
            df_sql = self.fetch_view_data(sql_n,'sqlserver')
            df_sf = self.fetch_view_data(sf_n,'snowflake')
            details = comp_inst.compare_results(self.normalize_dataframe(df_sf.copy()),self.normalize_dataframe(df_sql.copy()),sf_n,sql_n,'View')
            is_uniform = comp_inst.is_comparison_uniform(details)
            return {"sf_name": sf_n, "sql_name": sql_n, "details": details, "is_uniform": is_uniform}
        except Exception as e:
            return {"sf_name": sf_n, "sql_name": sql_n, "details": [{"Attribute":"Exec Error","Snowflake Output":f"Err:{e}","SQL Server Output":f"Err:{e}", "Comparison": "Error"}], "is_uniform": False}

    def compare_all_views(self) -> list:
        res=[]
        items=self.get_available_items()
        if not items: return [{"sf_name":"N/A", "sql_name":"N/A", "details":[{"Attribute":"Status","Snowflake Output":"-","SQL Server Output":"No common views found."}], "is_uniform": False}]
        comp_obj=Compare()
        for n in items: res.append(self._perform_comparison(n,comp_obj))
        if items: comp_obj.generate_comparison_html_from_structured_data("All_Views","View")
        return res

    def compare_specific_items(self, norm_names_list: list) -> list:
        res=[]
        if not norm_names_list: return [{"sf_name":"N/A", "sql_name":"N/A", "details":[{"Attribute":"Selection","Snowflake Output":"-","SQL Server Output":"No views selected."}], "is_uniform": False}]
        avail=self.get_available_items()
        comp_obj=Compare(); pc=0
        for n in norm_names_list:
            if n not in avail: res.append({"sf_name":f"SkipSF_{n}", "sql_name":f"SkipSQL_{n}", "details":[{"Attribute":"Skipped","Snowflake Output":"-","SQL Server Output":f"View '{n}' not common."}], "is_uniform": False}); continue
            res.append(self._perform_comparison(n,comp_obj)); pc+=1
        if pc>0: comp_obj.generate_comparison_html_from_structured_data(f"Selected_Views_{pc}_items","View")
        return res