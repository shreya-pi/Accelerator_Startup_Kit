#!/usr/bin/env python3
# from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG
import streamlit as st
from SP_Migration.scripts.log import log_info
import pyodbc
import snowflake.connector
from datetime import datetime
import pandas as pd
import os
import re


METADATA_TABLE = "procedures_metadata"
# if "show_metadata_table" not in st.session_state:
#     st.session_state.show_metadata_table = False


class CreateMetadataTable:
    def __init__(self, config:dict):
        if not config or "SNOWFLAKE_CONFIG" not in config:
            st.error("Configuration is missing or invalid.")
            st.stop()
        
        # Store the needed config parts as instance variables
        self.snowflake_config = config["SNOWFLAKE_CONFIG"]
        self.sql_server_config = config["SQL_SERVER_CONFIG"]



        if "show_metadata_table" not in st.session_state: st.session_state.show_metadata_table = False
        if 'staged_procedures' not in st.session_state:
            st.session_state.staged_procedures = []
        # A set to quickly check for duplicates before adding to the stage
        if 'staged_keys' not in st.session_state:
            st.session_state.staged_keys = set()

        pass



    def fetch_sqlserver_procedures(self):
        """Connects to SQL Server and returns a list of dicts with procedure metadata."""
        # cfg = SQL_SERVER_CONFIG
        sql_server_config = self.sql_server_config
    
        conn_str = f"DRIVER={sql_server_config['driver']};SERVER={sql_server_config['server']};DATABASE={sql_server_config['database']};UID={sql_server_config['username']};PWD={sql_server_config['password']}"
        rows = []
                # Initialize cnxn and cursor to None to handle connection errors
        cnxn = None
        cursor = None
        try:
            cnxn = pyodbc.connect(conn_str)
            cursor = cnxn.cursor()
        
            # 1) Routines
            # Get all stored procedures
            
            cursor.execute("""
                SELECT 
                  SPECIFIC_CATALOG AS dbname,
                  SPECIFIC_SCHEMA  AS schema_name,
                  SPECIFIC_NAME    AS procedure_name,
                  ROUTINE_DEFINITION AS procedure_definition
                FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
            """)
            procs = cursor.fetchall()
        
            # 2) Parameters 
            # Get all parameters for the stored procedures 
            cursor.execute("""
                SELECT
                  SPECIFIC_NAME      AS procedure_name,
                  PARAMETER_MODE     AS mode,
                  PARAMETER_NAME     AS name,
                  DATA_TYPE          AS data_type,
                  CHARACTER_MAXIMUM_LENGTH AS char_length
                FROM INFORMATION_SCHEMA.PARAMETERS
                ORDER BY SPECIFIC_NAME, ORDINAL_POSITION
            """)
            params = cursor.fetchall()
        
            # Group params by proc name
            # Create a dictionary where the key is the procedure name and the value is a list of parameter descriptions
            params_by_proc = {}
            for p in params:
                desc = f"{p.mode} {p.name} {p.data_type}"
                if p.char_length is not None:
                    desc += f"({p.char_length})"
                params_by_proc.setdefault(p.procedure_name, []).append(desc)
        
            # Build result
            # Create a list of dictionaries, each representing a procedure with its metadata and parameters
            # rows = []
            for r in procs:
                pname = r.procedure_name
                rows.append({
                    "SOURCE":               "SQLServer",
                    "DBNAME":               r.dbname,
                    "SCHEMA_NAME":          r.schema_name,
                    "PROCEDURE_NAME":       pname,
                    "PROCEDURE_DEFINITION": r.procedure_definition,
                    "PARAMETERS":           ", ".join(params_by_proc.get(pname, [])),
                })
        except pyodbc.Error as ex:
            # Catch potential database errors and report them
            sqlstate = ex.args[0]
            st.error(f"Database error in fetch_sqlserver_procedures: {sqlstate}")
            st.error(ex)
            st.stop() # Stop execution if we can't connect

        finally:
            if cursor:
                cursor.close()
            if cnxn:
                cnxn.close()
        return rows
    
    def _add_procs_to_stage(self, procs_to_add):
        """Adds a list of procedures to the staging area, avoiding duplicates."""
        newly_added_count = 0
        for proc in procs_to_add:
            # Create a unique key for each procedure
            proc_key = (proc['SOURCE'], proc['DBNAME'], proc['SCHEMA_NAME'], proc['PROCEDURE_NAME'])
            if proc_key not in st.session_state.staged_keys:
                st.session_state.staged_keys.add(proc_key)
                st.session_state.staged_procedures.append(proc)
                newly_added_count += 1
        
        if newly_added_count > 0:
            st.toast(f"Added {newly_added_count} new procedures to the staging area.", icon="✅")
        else:
            st.toast("No new, unique procedures were found to add to the stage.", icon="ℹ️")



    def parse_procedures_from_files(self, uploaded_files, dbname, schema_name):
        """Parses uploaded SQL files to extract procedure metadata."""
        rows = []
        if not uploaded_files:
            return rows
        
        # Regex to find the parameter block between parentheses after the procedure name.
        # It looks for CREATE/ALTER PROCEDURE, then the name, then captures the content
        # inside the parentheses, but only if it's followed by AS or BEGIN.
        param_pattern = re.compile(
            r"(?:CREATE|ALTER)\s+(?:PROCEDURE|PROC)\s+.*?"  # Match CREATE/ALTER PROC header
            r"(?:\((.*?)\))?"                              # OPTIONALLY match and capture params inside ()
            r"\s*(?:AS|BEGIN)",                            # Must be followed by AS or BEGIN
            re.IGNORECASE | re.DOTALL
        )

        for uploaded_file in uploaded_files:
            try:
                procedure_name = os.path.splitext(uploaded_file.name)[0]
                procedure_definition = uploaded_file.getvalue().decode("utf-8")

                parameters_str = ""
                match = param_pattern.search(procedure_definition)

                # if match:
                if match and match.group(1) is not None:
                    # Group 1 contains the content inside the parentheses.
                    raw_params = match.group(1).strip()
                    if raw_params:
                        # Normalize whitespace: replace any sequence of whitespace chars
                        # (including newlines, tabs) with a single space, then strip again.
                        parameters_str = re.sub(r'\s+', ' ', raw_params).strip()
                
                rows.append({
                    "SOURCE": "File Upload",
                    "DBNAME": dbname,
                    "SCHEMA_NAME": schema_name,
                    "PROCEDURE_NAME": procedure_name,
                    "PROCEDURE_DEFINITION": procedure_definition,
                    "PARAMETERS": parameters_str, 
                })
            except Exception as e:
                st.warning(f"Could not process file {uploaded_file.name}: {e}")
        
        return rows

    #     """Connects to SQL Server and returns a list of dicts with procedure metadata."""
    def load_into_snowflake(self,proc_list):
        """Creates the target table if needed and bulk‐loads procedure metadata."""
        sf_cfg = self.snowflake_config
        inserted_count = 0
        updated_count = 0
        ctx = snowflake.connector.connect(
            user=sf_cfg['user'],
            password=sf_cfg['password'],
            account=sf_cfg['account'],
            warehouse=sf_cfg['warehouse'],
            database=sf_cfg['database'],
            schema=sf_cfg['schema'],
            role=sf_cfg['role']
        )
        cs = ctx.cursor()
        try:
            # create table
            # Check if the table already exists
            cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
              SOURCE                STRING,
              DBNAME                STRING,
              SCHEMA_NAME           STRING,
              PROCEDURE_NAME        STRING,
              PROCEDURE_DEFINITION  STRING,
              CONVERSION_FLAG       BOOLEAN,
              LOAD_TIMESTAMP        TIMESTAMP_NTZ(9),
              SNOWFLAKE_DBNAME      STRING,
              SNOWFLAKE_SCHEMA_NAME STRING,
              SNOWFLAKE_DDL          STRING,
              PARAMETERS            STRING,
              IS_DEPLOYED           BOOLEAN,
              ERRORS                STRING
            )
            """)
        
                # --- MODIFIED LOGIC: Use MERGE instead of INSERT ---
            merge_sql = f"""
                MERGE INTO {METADATA_TABLE} AS T
                USING (
                    SELECT
                        %s AS SOURCE,
                        %s AS DBNAME,
                        %s AS SCHEMA_NAME,
                        %s AS PROCEDURE_NAME,
                        %s AS PROCEDURE_DEFINITION,
                        %s AS PARAMETERS
                ) AS S
                ON T.SCHEMA_NAME = S.SCHEMA_NAME AND T.PROCEDURE_NAME = S.PROCEDURE_NAME
                WHEN MATCHED THEN
                    UPDATE SET
                        T.PROCEDURE_DEFINITION = S.PROCEDURE_DEFINITION,
                        T.PARAMETERS = S.PARAMETERS,
                        T.LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (
                        SOURCE, DBNAME, SCHEMA_NAME, PROCEDURE_NAME, PROCEDURE_DEFINITION,
                        PARAMETERS, CONVERSION_FLAG, LOAD_TIMESTAMP, SNOWFLAKE_DBNAME,
                        SNOWFLAKE_SCHEMA_NAME, SNOWFLAKE_DDL, IS_DEPLOYED, ERRORS
                    )
                    VALUES (
                        S.SOURCE, S.DBNAME, S.SCHEMA_NAME, S.PROCEDURE_NAME, S.PROCEDURE_DEFINITION,
                        S.PARAMETERS, %s, CURRENT_TIMESTAMP(), %s,
                        %s, '', FALSE, ''
                    )
                """
    
    
    
    
            # insert_sql = f"""
            # INSERT INTO {METADATA_TABLE} (
            #   SOURCE, DBNAME, SCHEMA_NAME, PROCEDURE_NAME, PROCEDURE_DEFINITION,
            #   CONVERSION_FLAG, LOAD_TIMESTAMP,
            #   SNOWFLAKE_DBNAME, SNOWFLAKE_SCHEMA_NAME, SNOWFLAKE_DDL,
            #   PARAMETERS, IS_DEPLOYED, ERRORS
            # ) VALUES (
            #   %s, %s, %s, %s, %s,
            #   %s, CURRENT_TIMESTAMP(),
            #   %s, %s, %s,
            #   %s, %s, %s
            # )
            # """
            # inserted_count = 0
            # updated_count = 0
    
        
            for p in proc_list:
            #     cs.execute(merge_sql, (
            #         p["SOURCE"],
            #         p["DBNAME"],
            #         p["SCHEMA_NAME"],
            #         p["PROCEDURE_NAME"],
            #         p["PROCEDURE_DEFINITION"],
            #         False,                      # CONVERSION_FLAG
            #         sf_cfg['database'],         # SNOWFLAKE_DBNAME
            #         sf_cfg['schema'],           # SNOWFLAKE_SCHEMA_NAME
            #         "",                         # SNOWFLAKE_DDL placeholder
            #         p["PARAMETERS"],
            #         False,                       # IS_DEPLOYED
            #         ""                          # ERRORS
            #     ))

                params_tuple = (
                    # For USING clause (6 items)
                    p["SOURCE"],
                    p["DBNAME"],
                    p["SCHEMA_NAME"],
                    p["PROCEDURE_NAME"],
                    p["PROCEDURE_DEFINITION"],
                    p["PARAMETERS"],
                    # For WHEN NOT MATCHED -> INSERT clause (3 items)
                    False,                      # CONVERSION_FLAG
                    sf_cfg['database'],         # SNOWFLAKE_DBNAME
                    sf_cfg['schema']            # SNOWFLAKE_SCHEMA_NAME
                )
                cs.execute(merge_sql, params_tuple)
    

                result_scan_query = 'SELECT "number of rows inserted", "number of rows updated" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))'
                cs.execute(result_scan_query)
                
                # Fetch the single row of results
                result_row = cs.fetchone()
                if result_row:
                    rows_inserted, rows_updated = result_row
                    inserted_count += rows_inserted
                    updated_count += rows_updated
        

            ctx.commit()
            log_info(f"   → New procedures inserted: {inserted_count}")
            log_info(f"   → Existing procedures updated: {updated_count}")
            st.success(f"Load complete! Inserted: {inserted_count}, Updated: {updated_count}")

                            # Clear the stage on successful load
            st.session_state.staged_procedures = []
            st.session_state.staged_keys = set()
            st.session_state.show_metadata_table = True # Show the table after loading
        finally:
            cs.close()
            ctx.close()
    


    def show_metadata_table(self):
        """Displays the metadata table from Snowflake."""
        ctx = snowflake.connector.connect(**self.snowflake_config)

        try:
            df = pd.read_sql(f"SELECT * FROM {METADATA_TABLE} ORDER BY DBNAME, SCHEMA_NAME, PROCEDURE_NAME", ctx)

            st.dataframe(
                df,
                use_container_width=True,
                column_config={
                    "CONVERSION_FLAG": st.column_config.TextColumn(
                        "Conversion Flag",
                        help="Set to `True` to mark this procedure for migration."
                    ),
                    "IS_DEPLOYED": st.column_config.TextColumn(
                        "Is Deployed?",
                        help="Indicates if the converted procedure has been deployed in Snowflake."
                    )
                    # Add any other boolean columns here if needed
                },
                hide_index=True # Hides the pandas index for a cleaner look
            )

            # st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"❌ Failed to fetch data: {e}")
        finally:
            # cs.close()
            ctx.close()
    



    def run_etl_sync(self):
        """
        This is the HEAVY ACTION. It performs the full sync and should only be
        run when the user explicitly asks for it.
        """
        with st.spinner("Connecting to databases and syncing metadata... This may take a moment."):
            log_info("🔍 Fetching procedures from SQL Server…")
            procs = self.fetch_sqlserver_procedures()
            log_info(f"   → {len(procs)} procedures found.")
            
            if procs:
                log_info("⏫ Loading metadata into Snowflake…")
                self.load_into_snowflake(procs)
            else:
                log_info("   → No procedures to load.")
        
        st.success("✅ Metadata sync complete.")
        log_info("✅ Done.")




    def create_metadata_table(self):
        """Renders UI to collect procedures from multiple sources and load them."""
        st.subheader("1. Add Procedures to Staging Area")
        st.write("You can add procedures from SQL Server, file uploads, or both. They will be collected in a staging area before being loaded into Snowflake.")

        # --- Source 1: SQL Server ---
        if self.sql_server_config:
            with st.container(border=True):
                st.markdown("##### Source: SQL Server")
                if st.button("Fetch Procedures from SQL Server", use_container_width=True):
                    with st.spinner("Connecting to SQL Server..."):
                        procs_from_db = self.fetch_sqlserver_procedures()
                        self._add_procs_to_stage(procs_from_db)

        # --- Source 2: File Upload ---
        with st.container(border=True):
            st.markdown("##### Source: File Upload")
            with st.form("file_upload_form"):
                st.markdown("Upload one or more `.sql` files. The filename (without extension) will be used as the procedure name.")
                col1, col2 = st.columns(2)
                default_dbname = col1.text_input("Source Database Name", help="Logical DB name for these file-based procs.")
                default_schema = col2.text_input("Source Schema Name", "dbo", help="Logical schema name for these file-based procs.")
                
                uploaded_files = st.file_uploader("Upload Scripts", type=['sql'], accept_multiple_files=True, label_visibility="collapsed")
                
                if st.form_submit_button("Add Uploaded Files to Stage", use_container_width=True):
                    if not uploaded_files: st.warning("Please upload at least one `.sql` file.")
                    elif not default_dbname or not default_schema: st.warning("Please provide a source Database and Schema name.")
                    else:
                        procs_from_files = self.parse_procedures_from_files(uploaded_files, default_dbname, default_schema)
                        self._add_procs_to_stage(procs_from_files)
        
        st.divider()

        # --- Staging Area and Load Action ---
        st.subheader("2. Load Staged Procedures")
        if not st.session_state.staged_procedures:
            st.info("The staging area is empty. Add procedures from a source above.")
        else:
            num_staged = len(st.session_state.staged_procedures)
            st.success(f"**{num_staged}** procedure(s) are in the staging area, ready to be loaded.")

            with st.expander("View Staged Procedures"):
                st.dataframe(pd.DataFrame(st.session_state.staged_procedures), use_container_width=True, hide_index=True)
            
            load_col, clear_col = st.columns(2)
            if load_col.button(f"**Prepare {num_staged} Procedures for Conversion**", type="primary", use_container_width=True):
                self.load_into_snowflake(st.session_state.staged_procedures)
            
            if clear_col.button("Clear Staging Area", use_container_width=True):
                st.session_state.staged_procedures = []
                st.session_state.staged_keys = set()
                st.toast("Staging area cleared.", icon="🗑️")
                st.rerun() # Rerun to update the display immediately
        
        st.divider()

        # --- View Final Table ---
        st.subheader(f"3. View `{METADATA_TABLE}` Table")
        st.write(f"This table contains all procedures loaded from SQL Server and file uploads. You can view, filter, and search the metadata.")
        if st.button("📋 **Refresh/Show Metadata Table**", use_container_width=True):
            st.session_state.show_metadata_table = not st.session_state.show_metadata_table
        
        if st.session_state.show_metadata_table:
            self.show_metadata_table()







        # """
        # This is the LIGHTWEIGHT UI method. It just draws buttons and tables
        # based on the current session state. It's fast and can be run on every
        # rerun without issue.
        # """
        # st.subheader("Actions")
        
        # # --- BUTTON 1: The Heavy Action ---
        # if st.button("🔄 Sync Metadata from SQL Server", use_container_width=True):
        #     self.run_etl_sync()
        #     # After a sync, we probably want to see the table.
        #     st.session_state.show_metadata_table = True
        #     # st.rerun() # Optional: force an immediate rerun to reflect the state change

        # st.markdown("---")

        # # --- BUTTON 2: The UI Toggle ---
        # # Change the button text based on the current state for better UX
        # button_text = "Hide Full Metadata Table" if st.session_state.show_metadata_table else "📋 Show Full Metadata Table"
        
        # toggle = st.button(
        #     button_text,
        #     use_container_width=True,
        #     help=f"View the entire contents of the `{METADATA_TABLE}` table from Snowflake."
        # )

        # if toggle:
        #     # This button's ONLY job is to flip the state variable.
        #     st.session_state.show_metadata_table = not st.session_state.show_metadata_table
        
        # # --- CONDITIONAL RENDERING ---
        # # This part just checks the state and calls the display function if needed.
        # if st.session_state.show_metadata_table:
        #     # st.subheader("Full Metadata Table")
        #     self.show_metadata_table()
    



# if __name__ == "__main__":
#     # CreateMetadataTable().create_metadata_table()
#     try:
#     # Step 1: Import the config variables from the config.py file
#     # This works because app.py created this file in the root directory.
#         from config import SNOWFLAKE_CONFIG, SQL_SERVER_CONFIG
    
#         # Step 2: Assemble the config dictionary that the class expects
#         app_config = {
#             "SNOWFLAKE_CONFIG": SNOWFLAKE_CONFIG,
#             "SQL_SERVER_CONFIG": SQL_SERVER_CONFIG
#         }
    
#         # Step 3: Now, instantiate the class WITH the config dictionary
#         metadata_creator = CreateMetadataTable(config=app_config)
#         metadata_creator.create_metadata_table()
        
#         print("Script finished successfully.")

#     except ImportError:
#         # This error will happen if config.py doesn't exist.
#         print("ERROR: config.py not found.")
#         print("This script is intended to be run by the main Streamlit app after config has been uploaded.")
#         exit(1) # Exit with a non-zero status code to indicate failure
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         exit(1)
    
    