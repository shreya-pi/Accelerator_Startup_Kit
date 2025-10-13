# --- START OF FILE update_flag_st.py ---

import streamlit as st
import snowflake.connector
import pandas as pd
import os
import sys
import importlib
from SP_Migration.scripts.log import log_error, log_info

# Simple fallback for logging
# def log_error(msg): print(f"ERROR: {msg}")
# def log_info(msg): print(f"INFO: {msg}")

METADATA_TABLE = "procedures_metadata"

class SelectProcedures:
    def __init__(self, config: dict):
        """
        Initialize the SelectProcedures class with the provided configuration.
        """
        if not config or "SNOWFLAKE_CONFIG" not in config:
            raise ValueError("Configuration is missing or invalid.")
        # if "SQL_OUTPUT_DIR" not in config:
        #     raise ValueError("Configuration is missing 'SQL_OUTPUT_DIR'.")
        
        self.snowflake_config = config["SNOWFLAKE_CONFIG"]
        # self.output_dir = config["SQL_OUTPUT_DIR"]
        output_dir = "./extracted_procedures"
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_procedures(self):
        """
        Fetches procedures with CONVERSION_FLAG=TRUE from Snowflake
        and writes them to .sql files.
        """
        if "sf_conn" not in st.session_state or st.session_state.sf_conn.is_closed():
            st.error("❌ Snowflake connection is not active. Please 'Start Flow' first.")
            return

        ctx = st.session_state.sf_conn
        # Use a new cursor for this self-contained operation
        with ctx.cursor() as cs:
            try:
                os.makedirs(self.output_dir, exist_ok=True)
                log_info(f"Ensured output directory exists: {self.output_dir}")

                query = f"SELECT PROCEDURE_NAME, PROCEDURE_DEFINITION FROM {METADATA_TABLE} WHERE CONVERSION_FLAG = TRUE"
                rows = cs.execute(query).fetchall()

                if not rows:
                    st.warning("⚠️ No procedures are currently flagged for conversion. Nothing to extract.")
                    return

                extracted_count = 0
                for proc_name, definition in rows:
                    safe_name = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in proc_name)
                    file_path = os.path.join(self.output_dir, f"{safe_name}.sql")
            
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(definition.strip() + "\n")
                    
                    log_info(f"Wrote {proc_name} → {file_path}")
                    extracted_count += 1
                
                st.success(f"✅ **Extraction Complete!** {extracted_count} procedure(s) saved to `{self.output_dir}`. You are now ready for the **'4. Convert Procedures'** step.")

            except Exception as e:
                st.error(f"❌ An error occurred during extraction: {e}")

    def run_update_flag(self):
        """
        Encapsulates the Streamlit UI and logic for selecting procedures for conversion.
        """
        # Initialize all necessary session state variables
        if "flow_started" not in st.session_state: st.session_state.flow_started = False
        if "show_metadata_table" not in st.session_state: st.session_state.show_metadata_table = False
        if "search_terms" not in st.session_state: st.session_state.search_terms = {}

        # --- 1. INITIAL STATE: Display a welcome and start button ---
        if not st.session_state.flow_started:
            with st.container(border=True):
                st.subheader("🏁 Start Here")
                st.markdown(
                    """
                    This component allows you to select which SQL Server stored procedures you want to migrate.
                    - It connects to your Snowflake database to read the `procedures_metadata` table.
                    - It then displays the procedures, allowing you to flag them for conversion.
                    """
                )
                if st.button("▶️ **Start Flow**", help="Connect to Snowflake and load the procedure list"):
                    with st.spinner("Connecting to Snowflake and fetching procedures..."):
                        try:
                            ctx = snowflake.connector.connect(**self.snowflake_config)
                            st.session_state.sf_conn = ctx
                            st.session_state.sf_cursor = ctx.cursor()
                        except Exception as e:
                            st.error(f"❌ Snowflake connection failed: {e}"); st.stop()
                        
                        try:
                            fetch_sql = f"SELECT DBNAME, SCHEMA_NAME, PROCEDURE_NAME, CONVERSION_FLAG FROM {METADATA_TABLE} ORDER BY 1, 2, 3"
                            rows = st.session_state.sf_cursor.execute(fetch_sql).fetchall()
                            if not rows:
                                st.warning(f"⚠️ `{METADATA_TABLE}` is empty. Please run '1. Create Metadata Table' first."); st.stop()
                            
                            st.session_state.proc_map = {f"{db}.{sch}.{proc}": bool(flag) for db, sch, proc, flag in rows}


                                                        # --- OPTIMIZATION: DO HEAVY WORK ONCE AND CACHE IT ---
                            grouped = {}
                            for full_name, _ in st.session_state.proc_map.items():
                                db, schema, proc = full_name.split(".", 2)
                                # Pre-process the data for faster searching later
                                proc_data = {
                                    'proc_name': proc,
                                    'proc_lower': proc.lower(), # Pre-calculate lowercase
                                    'full_name': full_name
                                }
                                grouped.setdefault((db, schema), []).append(proc_data)
                            # Store the efficiently structured data in session state
                            st.session_state.grouped_procs = grouped


                            st.session_state.flow_started = True
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Failed to fetch procedures: {e}")
                            st.session_state.sf_cursor.close(); st.session_state.sf_conn.close(); st.stop()
            st.stop()

        # --- 2. ACTIVE STATE: Display selectors and action buttons ---
        proc_map = st.session_state.proc_map
        
        with st.container(border=True):
            st.subheader("🎯 Select Procedures for Conversion")
            st.caption("Check the box next to each procedure you want to convert. This sets its `CONVERSION_FLAG` to `TRUE` in the metadata table.")

            
            # grouped = {}
            # for full_name, _ in proc_map.items():
            #     db, schema, proc = full_name.split(".", 2)
            #     grouped.setdefault((db, schema), []).append((proc, full_name))
            
            
            # for (db, schema), items in sorted(grouped.items()):
            for (db, schema), items in sorted(st.session_state.grouped_procs.items()):
                expander_key = f"{db}.{schema}"
                with st.expander(f"**Database:** `{db}` → **Schema:** `{schema}` ({len(items)} procedures)"):
                                       # --- NEW: In-expander search bar ---

                    search_term = st.text_input(
                        "🔎 Filter procedures in this schema",
                        key=f"search_{expander_key}",
                        placeholder="e.g., GET_CUSTOMER_DETAILS",
                        on_change=lambda: None # This empty callback triggers a rerun on every keystroke
                    ).lower()
                    
                                        # Perform a very fast search on the pre-calculated lowercase names
                    filtered_items = [
                        item for item in items 
                        if search_term in item['proc_lower']
                    ]

                    # filtered_items = [(p, fn) for p, fn in items if search_term in p.lower()]
                    if not filtered_items: st.info("No procedures match your filter.")
                    else:
                        # Create a scrollable container for the list of checkboxes
                        with st.container(height=300):
                            for item in sorted(filtered_items, key=lambda x: x['proc_name']):
                                st.checkbox(
                                    label=item['proc_name'], 
                                    key=f"chk_{item['full_name']}", 
                                    value=proc_map[item['full_name']]
                                )

                    
                    

        # --- MODIFIED: A guided, step-by-step action container ---
        with st.container(border=True):
            st.subheader("⚙️ Save and Extract")
            # st.markdown(
            #     """
            #     Follow these two steps in order to save your selections and then create the necessary SQL files for the next component.
            #     """
            # )

            # --- Step 1: Update Flags ---
            st.markdown("#### Step 1: Save Your Changes")
            # st.caption("First, save any checkbox changes you made above. This action updates the `CONVERSION_FLAG` in the Snowflake database.")
            
            if st.button("📝 **Update Conversion Flags**", use_container_width=True, help="Saves your checkbox selections to the database."):
                to_update = []
                for full_name, orig_flag in proc_map.items():
                    new_flag = st.session_state[f"chk_{full_name}"]
                    if new_flag != orig_flag:
                        db, schema, proc = full_name.split(".", 2); to_update.append((new_flag, db, schema, proc))
                
                if not to_update:
                    st.info("🔎 No changes detected. Nothing to update.")
                else:
                    with st.spinner("⏳ Saving changes to Snowflake..."):
                        try:
                            update_sql = f"UPDATE {METADATA_TABLE} SET CONVERSION_FLAG = %s WHERE DBNAME = %s AND SCHEMA_NAME = %s AND PROCEDURE_NAME = %s"
                            st.session_state.sf_cursor.executemany(update_sql, to_update)
                            st.session_state.sf_conn.commit()
                            for new_flag, db, schema, proc in to_update:
                                st.session_state.proc_map[f"{db}.{schema}.{proc}"] = new_flag
                            st.success(f"✅ **Flags Updated!** {len(to_update)} procedure(s) were changed. You can now proceed to Step 2.")
                        except Exception as e:
                            st.error(f"❌ Error during update: {e}"); st.session_state.sf_conn.rollback()

            st.markdown("---") # Visual separator between steps

            # --- Step 2: Extract Procedures ---
            st.markdown("#### Step 2: Extract Procedures")
            # st.caption("After saving, extract all procedures that are currently flagged for conversion. This creates the `.sql` files needed for the next component.")

            if st.button("🚀 **Extract Selected Procedures**", use_container_width=True, help="Finds all procedures with CONVERSION_FLAG = TRUE and saves their SQL code."):
                with st.spinner(f"Extracting procedures to `{self.output_dir}`..."):
                    self.extract_procedures()


        
        st.markdown("---")
        
        with st.container(border=True):
            st.subheader("🔍 Data Inspection & Session Management")
            col3, col4 = st.columns(2)
            with col3:
                if st.button("📋 **Show/Hide Full Metadata Table**", use_container_width=True, help=f"View the entire contents of the `{METADATA_TABLE}` table from Snowflake."):
                    # Toggle the visibility of the metadata table
                    st.session_state.show_metadata_table = not st.session_state.get("show_metadata_table", False) 

            with col4:
                if st.button("🔒 **Close Connection & Restart**", use_container_width=True, help="Disconnect from Snowflake and reset this component."):
                    try:
                        st.session_state.sf_cursor.close(); st.session_state.sf_conn.close()
                    except Exception: pass
                    keys_to_delete = [k for k in st.session_state.keys() if k.startswith(("chk_", "flow_", "proc_", "sf_", "show_"))]
                    for key in keys_to_delete: del st.session_state[key]
                    st.rerun()

            if st.session_state.get("show_metadata_table", False):
                with st.spinner("Fetching latest metadata from Snowflake..."):
                    try:
                        df = pd.read_sql(f"SELECT * FROM {METADATA_TABLE} ORDER BY DBNAME, SCHEMA_NAME, PROCEDURE_NAME", st.session_state.sf_conn)
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



