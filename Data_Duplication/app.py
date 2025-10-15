import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector import connect
# from snowflake.connector.pandas_tools import fetch_pandas_all
# from config import SNOWFLAKE_CONFIG
from Data_Duplication.col_desc import SnowflakeSchemaDescriber
from Data_Duplication.dmf_definitions import get_dmf_functions


class DataDuplicatesApp:
    def __init__(self):
        self.conn = None
        self.describer = None

    def init_connection(self, SNOWFLAKE_CONFIG):
        try:
            sf_cfg = SNOWFLAKE_CONFIG
            self.conn = snowflake.connector.connect(
                user=sf_cfg['user'],
                password=sf_cfg['password'],
                account=sf_cfg['account'],
                warehouse=sf_cfg['warehouse'],
                database=sf_cfg['database'],
                schema=sf_cfg['schema'],
                role=sf_cfg.get('role', None)
            )
            return self.conn
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            return None

    def get_describer(self):
        try:
            self.describer = SnowflakeSchemaDescriber()
            return self.describer
        except Exception as e:
            st.error(f"Failed to initialize the Column Describer: {e}")
            return None

    @staticmethod
    def get_schema_text():
        try:
            with open('Data_Duplication/input_and_output_files/formatted_schema.md', 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            st.error("Error: `schema.txt` not found. Please create it in the root directory.")
            return None

    @staticmethod
    def run_query(_conn, query: str) -> pd.DataFrame:
        with _conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)

    # @staticmethod
    # def get_tables(_conn):
    #     if _conn:
    #         try:
    #             schema_name = SNOWFLAKE_CONFIG['schema']
    #             database_name = SNOWFLAKE_CONFIG['database']
    #             cursor = _conn.cursor()
    #             cursor.execute(f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}")
    #             tables = [row[1] for row in cursor.fetchall()]
    #             return tables
    #         except snowflake.connector.Error as e:
    #             st.error(f"Error fetching tables: {e}")
    #             return []
    #     return []

    @staticmethod
    def get_columns_for_table(_conn, table_name):
        if _conn and table_name:
            try:
                cursor = _conn.cursor()
                cursor.execute(f"DESCRIBE TABLE {table_name}")
                columns = [row[0] for row in cursor.fetchall()]
                return columns
            except snowflake.connector.Error as e:
                st.error(f"Error describing table: {e}")
                return []
        return []

    @staticmethod
    def execute_dmf(_conn, function_sql, table_name):
        if _conn:
            try:
                query = f'SELECT {function_sql} AS "Result" FROM "{table_name}"'
                st.info(f"Executing Query: `{query}`")
                result_df = pd.read_sql(query, _conn)
                return result_df
            except Exception as e:
                st.error(f"Error executing DMF: {e}")
                return None
        return None

    def show_dmf_controls(self, selected_table_dmf):
        st.sidebar.markdown("---")
        st.sidebar.header("DMF Controls")
        st.sidebar.info("Select a DMF function to analyze data quality.")

        dmf_functions = get_dmf_functions()
        selected_function_name = st.sidebar.selectbox("1. Select a DMF Function", list(dmf_functions.keys()))
        function_template = dmf_functions[selected_function_name]
        requires_column = "{column}" in function_template
        selected_column = None

        if requires_column:
            columns = self.get_columns_for_table(self.conn, selected_table_dmf)
            if columns:
                selected_column = st.sidebar.selectbox("2. Select a Column", columns, help="This function requires a column.")
            else:
                st.sidebar.warning("Could not retrieve columns for this table.")
        else:
            st.sidebar.info("This function does not require a column selection.")

        st.header(f"DMF Functions: `{selected_function_name}`")
        st.markdown("---")

        can_proceed = (requires_column and selected_column) or not requires_column
        if not can_proceed:
            st.warning("Please select a column in the sidebar to run this function.")
        else:
            if requires_column:
                final_function_sql = function_template.format(column=f'"{selected_column}"')
            else:
                final_function_sql = function_template

            if st.button(f"▶️ Run on `{selected_table_dmf}`"):
                query = f'SELECT {final_function_sql} AS "Result" FROM {selected_table_dmf}'
                result_df = self.run_query(self.conn, query)

                if result_df is not None:
                    st.success("Query executed successfully!")
                    if result_df.shape[0] == 1 and result_df.shape[1] == 1:
                        metric_label = f"{selected_function_name}"
                        if selected_column:
                            metric_label += f" of {selected_column} column in `{selected_table_dmf}`"
                        st.metric(label=metric_label, value=result_df.iloc[0,0])
                    else:
                        st.dataframe(result_df, width='stretch')
                else:
                    st.error("Failed to execute the query. Check the error message above.")

    def describe_table_columns(self, selected_table):
        st.subheader(f"AI-Generated Column Descriptions for `{selected_table}`")
        with st.spinner(f"Generating description for {selected_table}... This may take a moment."):
            try:
                describer = self.get_describer()
                schema_blob = self.get_schema_text()
                if describer and schema_blob:
                    filtered_schema = describer.filter_schema(selected_table, schema_blob)
                    description = describer.describe_with_cortex(filtered_schema, model='snowflake-llama-3.3-70b')
                    st.markdown(description)
                else:
                    st.error("Could not proceed. Check for initialization errors above.")
            except Exception as e:
                st.error(f"An unexpected error occurred during description generation: {e}")
                st.error("Please ensure `col_desc.py`, `sf_config.py`, and `schema.txt` are set up correctly.")

    def data_quality_analysis(self, selected_table, database_name, schema_name):
        st.subheader("Data Quality Summary")
        col1, col2 = st.columns(2)
        try:
            total_query = f"SELECT COUNT(*) FROM {database_name}.{schema_name}.{selected_table};"
            total_df = self.run_query(self.conn, total_query)
            total_records = total_df.iloc[0, 0] if not total_df.empty else 0
            with col1:
                st.metric(label=f"Total Records in `{selected_table}`", value=int(total_records))

            duplicate_view = f"{selected_table}_duplicate_records"
            dupe_count_query = f"SELECT COUNT(*) FROM {duplicate_view};"
            dupe_count_df = self.run_query(self.conn, dupe_count_query)
            total_duplicates = dupe_count_df.iloc[0, 0] if not dupe_count_df.empty else 0
            with col2:
                st.metric(label="Total Duplicate Records Found", value=int(total_duplicates))

            st.markdown("---")
            if total_duplicates > 0:
                st.info(f"Showing the top 50 duplicate records from `{duplicate_view}`.")
                dupe_data_query = f"SELECT * FROM {duplicate_view} LIMIT 50;"
                duplicates_df = self.run_query(self.conn, dupe_data_query)
                if not duplicates_df.empty:
                    st.dataframe(duplicates_df, width='stretch')
                else:
                    st.warning("Could not retrieve the list of duplicate records.")
            else:
                st.success("✅ No duplicate records were found in this table.")

            complete_table_name = f"{database_name}.{schema_name}.{selected_table}".strip('"')
            self.show_dmf_controls(complete_table_name)
        except Exception as e:
            st.error(f"An error occurred during analysis: {e}")

    def view_cleaned_data(self, selected_table, database_name, schema_name):
        st.subheader("Original vs. Cleaned Data Summary")
        col1, col2 = st.columns(2)
        try:
            total_query = f"SELECT COUNT(*) FROM {database_name}.{schema_name}.{selected_table};"
            total_df = self.run_query(self.conn, total_query)
            total_records = total_df.iloc[0, 0] if not total_df.empty else 0
            with col1:
                st.metric(label=f"Total Records in Original (`{selected_table}`)", value=int(total_records))

            view_name = f"{selected_table}_clean_view"
            count_query = f"SELECT COUNT(*) FROM {view_name};"
            count_df = self.run_query(self.conn, count_query)
            total_clean_records = count_df.iloc[0, 0] if not count_df.empty else 0
            with col2:
                st.metric(label=f"Filtered Records in Clean View (`{view_name}`)", value=int(total_clean_records))

            st.markdown("---")
            st.info(f"Showing the top 50 rows from the clean view (`{view_name}`).")
            data_query = f"SELECT * FROM {view_name} LIMIT 50;"
            clean_data_df = self.run_query(self.conn, data_query)
            if not clean_data_df.empty:
                st.dataframe(clean_data_df, width='stretch')
            else:
                st.warning("The clean view is empty or inaccessible.")

            self.show_dmf_controls(view_name)
        except Exception as e:
            st.error(f"An error occurred while querying: {e}")

    def run(self, config: dict):
        SNOWFLAKE_CONFIG = config.get("SNOWFLAKE_CONFIG")
        if not SNOWFLAKE_CONFIG:
            st.error("`SNOWFLAKE_CONFIG` was not found in the uploaded configuration file.")
            return
        # st.set_page_config(
        #     page_title="Snowflake Data Quality Dashboard",
        #     page_icon="❄️",
        #     layout="wide"
        # )
        st.title("❄️ Snowflake Data Quality Dashboard")
        st.write("This dashboard allows you to analyze data quality for specific tables.")

        self.conn = self.init_connection(SNOWFLAKE_CONFIG)
        database_name = SNOWFLAKE_CONFIG['database']
        schema_name = SNOWFLAKE_CONFIG['schema']
        if not self.conn:
            st.stop()

        TABLE_LIST = ["CUSTOMERS", "ORDERS", "PRODUCTS"]
        st.sidebar.header("Controls")
        selected_table = st.sidebar.selectbox(
            "Select a Table:",
            options=TABLE_LIST
        )
        action = st.sidebar.radio(
            "Choose an Action:",
            options=[
                "1. Data Quality Analysis",
                "2. View Cleaned Data",
                "3. Describe Table Columns"
            ]
        )
        st.header(f"Table: `{selected_table}`")
        st.markdown(f"**Action:** {action}")
        st.markdown("---")

        if action == "1. Data Quality Analysis":
            self.data_quality_analysis(selected_table, database_name, schema_name)
        elif action == "2. View Cleaned Data":
            self.view_cleaned_data(selected_table, database_name, schema_name)
        elif action == "3. Describe Table Columns":
            self.describe_table_columns(selected_table)










