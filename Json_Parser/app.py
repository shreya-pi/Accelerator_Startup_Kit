import streamlit as st
import pandas as pd
import os
import io
import sys
from contextlib import contextmanager

# Import the core functions from your existing script
# This assumes 'json_to_snowflake.py' and 'config.py' are in the same directory
try:
    from Json_Parser.json_to_snowflake import list_files_in_stage, process_json_from_stage_to_snowflake
    # from config import SNOWFLAKE_CONFIG
except ImportError as e:
    st.error(f"""
    **Error: Failed to import necessary modules.**

    Please ensure the following files are in the same directory as `app.py`:
    - `json_to_snowflake.py`
    - `config.py`

    Details: {e}
    """)
    st.stop()


# Helper to capture print statements for live logging
@contextmanager
def st_capture(output_func):
    with io.StringIO() as stdout, redirect_stdout(stdout):
        yield
        output_func(stdout.getvalue())

# A more robust context manager for stdout redirection
@contextmanager
def redirect_stdout(new_target):
    old_target, sys.stdout = sys.stdout, new_target
    try:
        yield
    finally:
        sys.stdout = old_target

# --- Streamlit App UI ---
class JsonToSnowflakeApp:
    def __init__(self):
        self.STATE_KEYS = [
            'files_in_stage', 'selected_file', 'process_logs',
            'final_results', 'error_message'
        ]

        self.init_session_state()

    def init_session_state(self):
        for key in self.STATE_KEYS:
            if key not in st.session_state:
                st.session_state[key] = None # or some default value

        if 'files_in_stage' not in st.session_state:
            st.session_state.files_in_stage = []
        if 'selected_file' not in st.session_state:
            st.session_state.selected_file = None
        if 'process_logs' not in st.session_state:
            st.session_state.process_logs = ""
        if 'final_results' not in st.session_state:
            st.session_state.final_results = None
        if 'error_message' not in st.session_state:
            st.session_state.error_message = ""

    def reset_state(self):
        """Safely resets only the state for this component."""
        for key in self.STATE_KEYS:
            if key in st.session_state:
                del st.session_state[key]
        self.init_session_state() # Re-initialize to default values

    def sidebar_config(self, config:dict):
        SNOWFLAKE_CONFIG = config.get('SNOWFLAKE_CONFIG', {})
        st.sidebar.header("Configuration")
        st.sidebar.info(f"Connecting as user **{SNOWFLAKE_CONFIG.get('user')}** to database **{SNOWFLAKE_CONFIG.get('database')}**.")
        stage_name = st.sidebar.text_input(
            "Snowflake Stage Name",
            value="@JSON_STAGE",
            help="Enter the full stage name, e.g., '@MY_STAGE' or '@DB.SCHEMA.MY_STAGE'"
        )
        return stage_name

    def list_files(self, stage_name,config:dict):
        self.reset_state()

        # st.session_state.files_in_stage = []
        # st.session_state.selected_file = None
        # st.session_state.process_logs = ""
        # st.session_state.final_results = None
        # st.session_state.error_message = ""

        if not stage_name:
            st.warning("Please enter a stage name.")
        else:
            try:
                with st.spinner(f"Connecting and listing files in `{stage_name}`..."):
                    files = list_files_in_stage(stage_name, config)
                    st.session_state.files_in_stage = [os.path.basename(f) for f in files]
                    if not st.session_state.files_in_stage:
                        st.warning(f"No JSON files (.json, .jsonl, .ndjson) found in stage `{stage_name}`.")
            except Exception as e:
                st.session_state.error_message = f"Failed to list files: {e}"


    def process_file(self, stage_name, selected_file, append_log,config:dict):
        st.session_state.process_logs = ""
        st.session_state.final_results = None
        st.session_state.error_message = ""
        try:
            with st.spinner("Processing file... This may take a moment. See logs below for progress."):
                with st_capture(append_log):
                    results = process_json_from_stage_to_snowflake(
                        stage_name=stage_name,
                        file_path_in_stage=selected_file, 
                        config=config  
                    )
                    st.session_state.final_results = results
        except Exception as e:
            append_log(f"\n--- FATAL ERROR ---\n{e}")
            st.session_state.error_message = f"An error occurred during processing: {e}"


    def show_results(self):
        if st.session_state.error_message and not st.session_state.final_results:
            st.error(st.session_state.error_message)

        if st.session_state.final_results is not None:
            st.success(f"Successfully processed **{st.session_state.selected_file}**!")
            st.subheader("Process Summary")

            if not st.session_state.final_results:
                st.info("The process completed, but no tables were created or loaded.")
            else:
                summary_data = []
                for table, info in st.session_state.final_results.items():
                    summary_data.append({
                        "Table Name": table,
                        "Rows Loaded": info.get('rows_loaded', 'N/A'),
                        "Columns Created": info.get('columns', 'N/A')
                    })
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, width='stretch')

    def start_over_button(self):
        if st.session_state.files_in_stage or st.session_state.final_results or st.session_state.error_message:
            st.sidebar.markdown("---")
            if st.sidebar.button("Start Over"):
                st.session_state.clear()
                st.rerun()

    def run(self, config:dict):
        # st.set_page_config(page_title="JSON to Snowflake Loader", layout="wide", page_icon="❄️")
        st.title("❄️ JSON to Snowflake Relational Loader")
        st.markdown("This tool connects to Snowflake, lists JSON files in a specified stage, and processes a selected file into a normalized relational schema.")

        stage_name = self.sidebar_config(config)

        if st.sidebar.button("List Files in Stage", type="primary"):
            self.list_files(stage_name,config)

        if st.session_state.get('files_in_stage') is None:
             self.init_session_state()

        if st.session_state.error_message:
            st.error(st.session_state.error_message)

        if st.session_state.files_in_stage:
            st.subheader("Step 1: Select a File to Process")
            file_options = ["-- Select a file --"] + st.session_state.files_in_stage
            st.session_state.selected_file = st.selectbox(
                "Available JSON files:",
                options=file_options,
                index=0
            )

            if st.session_state.selected_file and st.session_state.selected_file != "-- Select a file --":
                st.subheader("Step 2: Start the Process")
                st.info(f"Ready to process: **{st.session_state.selected_file}**")

                if st.button(f"Normalize and Load '{st.session_state.selected_file}'", type="primary"):
                    log_container = st.expander("Live Process Log", expanded=True)
                    log_placeholder = log_container.empty()

                    def append_log(log_text):
                        st.session_state.process_logs += log_text
                        log_placeholder.code(st.session_state.process_logs, language='log')

                    self.process_file(stage_name, st.session_state.selected_file, append_log, config)

        self.show_results()
        self.start_over_button()


