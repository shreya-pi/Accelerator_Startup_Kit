# --- START OF FILE app.py ---

import streamlit as st
import snowflake.connector
import concurrent.futures
from io import StringIO
import sys
import time
import uuid
import configparser

# --- Local Module Imports ---
import Teradata_Migration.migrator as migrator
import Teradata_Migration.snowflake_operations_1 as sf_ops



class TeradataMigrationApp:
    def __init__(self):
        self.st = st
        self.snowflake = snowflake
        self.concurrent = concurrent
        self.StringIO = StringIO
        self.sys = sys
        self.time = time
        self.uuid = uuid
        self.configparser = configparser
        self.migrator = migrator
        self.sf_ops = sf_ops
        self.session_state = st.session_state
        self.session_state.td_databases = []
        self.session_state.selected_db = None
        self.session_state.td_tables = []
        self.session_state.selected_tables = []
        self.session_state.migration_started = False
        self.session_state.migration_logs = {}
        self.session_state.migration_status = {}
        self.session_state.executor = None
        self.session_state.futures = []
        self.session_state.config = None

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_teradata_databases(_config):
        st.info("Connecting to Teradata to list databases...")
        try:
            databases = migrator.list_teradata_databases(_config)
            if not databases: st.error("Could not retrieve Teradata databases.")
            return databases
        except Exception as e:
            st.error(f"Error fetching Teradata databases: {e}")
            return []

    @staticmethod
    @st.cache_data(ttl=3600)
    def get_teradata_tables(_config, _selected_db):
        if not _selected_db: return []
        st.info(f"Connecting to Teradata to list tables for {_selected_db}...")
        try:
            tables = migrator.list_teradata_tables(_config, _selected_db)
            if not tables: st.warning(f"No tables found in '{_selected_db}'.")
            return tables
        except Exception as e:
            st.error(f"Error fetching Teradata tables for {_selected_db}: {e}")
            return []

    @staticmethod
    def connect_to_snowflake(config):
        try:
            sf_conn = snowflake.connector.connect(
                user=config['SNOWFLAKE']['USER'],
                password=config['SNOWFLAKE']['PASSWORD'],
                account=config['SNOWFLAKE']['ACCOUNT'],
                warehouse=config['SNOWFLAKE']['WAREHOUSE'],
                database=config['SNOWFLAKE']['DATABASE'],
                schema=config['SNOWFLAKE']['SCHEMA'],
                role=config['SNOWFLAKE']['ROLE']
            )
            return sf_conn, sf_conn.cursor()
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}", file=sys.stderr)
            return None, None

    @staticmethod
    def run_migration_for_table_wrapper(config, table_name, database_name, log_stream, migration_details, job_id):
        def stream_log_func(message):
            log_stream.write(message + "\n")

        log_stream.write(f"[{table_name}] Wrapper started.\n")

        sf_conn, sf_cursor = None, None
        audit_id = None
        migration_result = {}
        error_message = None

        try:
            sf_conn, sf_cursor = TeradataMigrationApp.connect_to_snowflake(config)
            if not sf_conn:
                raise Exception("Failed to establish Snowflake connection inside the thread.")

            watermark_value = sf_ops.get_last_watermark(sf_cursor, table_name, stream_log_func) if migration_details['type'] == 'Delta Load (Incremental)' else None
            audit_id = sf_ops.start_audit_log(sf_cursor, job_id, table_name, migration_details['type'], watermark_value)

            migration_result = migrator.migrate_table(config, table_name, database_name, sf_cursor, log_func=stream_log_func, migration_details=migration_details)

            success = migration_result.get("success", False)
            log_stream.write(f"[{table_name}] Wrapper finished with success={success}.\n")
            return table_name, success

        except Exception as e:
            error_message = str(e)
            log_stream.write(f"[{table_name}] CRITICAL ERROR IN WRAPPER: {e}\n")
            print(f"[{table_name}] Unhandled exception in wrapper: {e}", file=sys.stderr)
            return table_name, False

        finally:
            final_status_for_audit = "SUCCESS" if migration_result.get("success", False) else "FAILED"
            if sf_cursor:
                sf_ops.finish_audit_log(
                    sf_cursor,
                    audit_id=audit_id,
                    status=final_status_for_audit,
                    rows_processed=migration_result.get("rows_processed", 0),
                    watermark_end=None,
                    error_message=error_message
                )
                sf_cursor.close()
            if sf_conn:
                sf_conn.close()
            log_stream.write(f"[{table_name}] Snowflake connection closed.\n")
  

    def run(self, config:dict):
        st.sidebar.header("Configuration & Control")
        
        with st.sidebar:
            st.subheader("1. Load Configuration")
            # uploaded_file = st.file_uploader("Upload your config.ini file", type=['ini'])
            # if config is not None:
            #     try:
            #         # string_data = uploaded_file.getvalue().decode("utf-8")
            #         # config = configparser.ConfigParser()
            #         # config.read_string(string_data)
            #         if 'TERADATA' in config and 'SNOWFLAKE' in config and 'AZURE' in config:
            #             st.session_state.config = config
            #             st.success("Configuration loaded successfully!")
            #         else:
            #             st.error("Invalid config file. Ensure [TERADATA], [SNOWFLAKE], and [AZURE] sections exist.")
            #             st.session_state.config = None
            #     except Exception as e:
            #         st.error(f"Failed to parse config file: {e}")
            #         st.session_state.config = None


        st.session_state.config = config
        
        if st.session_state.config:
            config = st.session_state.config
            with st.sidebar:
                st.subheader("2. Select Teradata Database")
                if st.button("Load Teradata Databases"):
                    st.session_state.td_databases = self.get_teradata_databases(config)
                    st.session_state.selected_db = None
                if st.session_state.td_databases:
                    current_db_index = st.session_state.td_databases.index(st.session_state.selected_db) + 1 if st.session_state.selected_db and st.session_state.selected_db in st.session_state.td_databases else 0
                    new_selection = st.selectbox("Choose a Database", [""] + st.session_state.td_databases, index=current_db_index, key="db_selector")
                    if new_selection != st.session_state.selected_db:
                        st.session_state.selected_db = new_selection
                        st.session_state.td_tables, st.session_state.selected_tables = [], []
                        st.session_state.migration_started = False
                        if not st.session_state.migration_started:
                            st.session_state.migration_logs, st.session_state.migration_status = {}, {}
        
            with st.sidebar:
                st.subheader("3. Select Tables for Migration")
                if st.session_state.selected_db:
                    if st.button(f"Load Tables from {st.session_state.selected_db}"):
                        st.session_state.td_tables, st.session_state.selected_tables = self.get_teradata_tables(config, st.session_state.selected_db), []
                    if st.session_state.td_tables:
                        st.session_state.selected_tables = st.multiselect("Choose Tables", st.session_state.td_tables, default=st.session_state.selected_tables, key="table_selector")
                    else:
                        st.warning("No tables loaded.")
                else:
                    st.info("Please select a Teradata database first.")
        
            with st.sidebar:
                st.subheader("4. Start Migration")
                migration_type = st.radio("Select Migration Type", ('Full Load (Replaces table)', 'Delta Load (Incremental)'), key="migration_type_selector")
                primary_key_column = st.text_input("Enter primary key column name", key="pk_column_input").strip() if migration_type == 'Delta Load (Incremental)' else ""
                if migration_type == 'Delta Load (Incremental)':
                    st.info("Delta loads require a primary key. Assumes a `last_updated` column exists for tracking.")
                can_start = st.session_state.selected_tables and not st.session_state.migration_started
                if migration_type == 'Delta Load (Incremental)' and not primary_key_column:
                    can_start = False
                    st.warning("Please provide a primary key for Delta Load.")
                if can_start:
                    if st.button("Start Migration Process", type="primary"):
                        st.session_state.migration_started, st.session_state.job_id = True, str(uuid.uuid4())
                        st.session_state.migration_details = {"type": migration_type, "tracking_column": "last_updated", "primary_key_column": primary_key_column}
                        st.session_state.migration_logs = {table: StringIO() for table in st.session_state.selected_tables}
                        st.session_state.migration_status = {table: "Pending" for table in st.session_state.selected_tables}
                        max_workers = int(config.get('MIGRATOR', 'MAX_MIGRATION_WORKERS', fallback=5))
                        st.session_state.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
                        st.session_state.futures = []
                        st.rerun()
                elif st.session_state.migration_started:
                    st.info("Migration is in progress...")
                elif not st.session_state.selected_tables:
                    st.info("Select tables to enable migration.")
        
            st.header("Migration Progress")
            if st.session_state.migration_started:
                cols = st.columns(2)
                cols[0].metric("Tables Selected", len(st.session_state.selected_tables))
                cols[1].metric("Tables Completed", sum(1 for status in st.session_state.migration_status.values() if status.startswith(("Success", "Failed"))))
                progress_bar = st.progress(0)
                total_tables = len(st.session_state.selected_tables)
        
                table_progress_trackers = {table: {"status_placeholder": st.empty(), "log_expander": st.expander(f"Logs for {table}", expanded=False), "log_area": None} for table in st.session_state.selected_tables}
                for table in st.session_state.selected_tables:
                    table_progress_trackers[table]["status_placeholder"].markdown(f"**{table}**: `{st.session_state.migration_status.get(table, 'Pending')}`")
        
                if not st.session_state.futures and st.session_state.executor:
                    for table in st.session_state.selected_tables:
                        log_stream = st.session_state.migration_logs[table]
                        # --- CRITICAL FIX 2: Pass 'config' as the first argument to the wrapper ---
                        future = st.session_state.executor.submit(
                            self.run_migration_for_table_wrapper,
                            config,
                            table,
                            st.session_state.selected_db,
                            log_stream,
                            st.session_state.migration_details,
                            st.session_state.job_id
                        )
                        st.session_state.futures.append((table, future))
        
                completed_count = 0
                for table, future in st.session_state.futures:
                    with table_progress_trackers[table]["log_expander"]:
                        if table_progress_trackers[table]["log_area"] is None: table_progress_trackers[table]["log_area"] = st.empty()
                        st.session_state.migration_logs[table].seek(0)
                        table_progress_trackers[table]["log_area"].code(st.session_state.migration_logs[table].read())
                    if future.done():
                        completed_count += 1
                        try:
                            _table_name, success = future.result()
                            status_text, color = ("Success", "green") if success else ("Failed", "red")
                            st.session_state.migration_status[_table_name] = status_text
                        except Exception as e:
                            status_text, color = "Failed (Error during execution)", "red"
                            st.session_state.migration_status[table] = status_text
                            st.session_state.migration_logs[table].write(f"\n[CRITICAL ERROR] Migration process failed with exception: {e}")
                        table_progress_trackers[table]["status_placeholder"].markdown(f"**{table}**: <span style='color:{color}'>`{status_text}`</span>", unsafe_allow_html=True)
                    else:
                        table_progress_trackers[table]["status_placeholder"].markdown(f"**{table}**: `{st.session_state.migration_status.get(table, 'Running')}`")
        
                current_progress = int((completed_count / total_tables) * 100) if total_tables > 0 else 0
                progress_bar.progress(current_progress)
        
                if completed_count == total_tables:
                    st.success("All tables processed! Check MIGRATION_CONTROL.MIGRATION_AUDIT_LOG for details.")
                    st.session_state.migration_started = False
                    if st.session_state.executor:
                        st.session_state.executor.shutdown(wait=True)
                        st.session_state.executor, st.session_state.futures = None, []
        
                 
                else:
                    time.sleep(1)
                    st.rerun()
            elif st.session_state.migration_status and not st.session_state.migration_started:
                st.header("Migration Summary")
                success_count = sum(1 for s in st.session_state.migration_status.values() if s == "Success")
                failed_count = sum(1 for s in st.session_state.migration_status.values() if s.startswith("Failed"))
                st.write(f"**Total Tables Processed:** {success_count + failed_count}")
                st.write(f"**Successful Migrations:** {success_count}")
                st.write(f"**Failed Migrations:** {failed_count}")
                if failed_count > 0:
                    st.error("Some migrations failed. Check logs for details.")
                    failed_tables = [table for table, status in st.session_state.migration_status.items() if status.startswith("Failed")]
                    st.write("Failed Tables:")
                    for table in failed_tables: st.write(f"- {table}")
            else:
                st.info("Complete the steps in the sidebar to begin.")
        
        else:
            st.info("⬅️ Please upload a `config.ini` file in the sidebar to begin the migration process.")
    

















