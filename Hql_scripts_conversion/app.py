import streamlit as st
import snowflake.connector
import os
import subprocess
import time
from pathlib import Path
import tempfile
from datetime import datetime
import pandas as pd
from Hql_scripts_conversion.log import log_info, log_error
# from config import SNOWFLAKE_CONFIG


class HqlScriptsConverterApp:
    def __init__(self):
        self.st = st
        self.snowflake = snowflake
        self.os = os
        self.subprocess = subprocess
        self.time = time
        self.Path = Path
        self.tempfile = tempfile
        self.datetime = datetime
        self.pd = pd
        self.log_info = log_info
        self.log_error = log_error
        self.CORTEX_MODEL = "snowflake-llama-3.3-70b"
        # self.SNOWFLAKE_CONFIG = SNOWFLAKE_CONFIG
        self.CONVERSION_CONFIG = {
            "HQL": {
                "title": "HQL to SQL Conversion & Publishing Workflow",
                "description": "This tool automates the entire process of converting HQL scripts to SQL using Snowflake Cortex.",
                "find_str": "HQL",
                "source_repo": "my_repo_clone_source_scripts",
                "dest_repo": "my_repo_clone_converted_scripts",
                "github_url": "https://github.com/shreya-pi/HQL_Converted_Sample_Scripts.git",
                "output_dir": "Hql_scripts_conversion/Converted_Hql_Scripts",
                "prompt_file": "Hql_scripts_conversion/prompt.txt",
                "source_extension": ".hql",
            },
            "BTEQ": {
                "title": "BTEQ to SQL Conversion & Publishing Workflow",
                "description": "This tool automates the entire process of converting BTEQ scripts to SQL using Snowflake Cortex.",
                "find_str": "BTEQ",
                "source_repo": "my_repo_clone_bteq_scripts",
                "dest_repo": "my_repo_clone_converted_bteq_scripts",
                "github_url": "https://github.com/shreya-pi/BTEQ_Converted_Sample_Scripts.git",
                "output_dir": "Hql_scripts_conversion/Converted_BTEQ_Scripts",
                "prompt_file": "prompt_bteq.txt",
                "source_extension": ".bteq",
            }
        }

    @staticmethod
    @st.cache_resource
    def get_snowflake_connection(config):
        try:
            return snowflake.connector.connect(**config)
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            return None

    @staticmethod
    def list_files_in_git_repo(conn, repo_name, branch):
        with conn.cursor() as cur:
            cur.execute(f"LIST @{repo_name}/branches/{branch}/;")
            prefix_to_strip = f"{repo_name}/branches/{branch}/"
            return [row[0].replace(prefix_to_strip, "", 1) for row in cur]

    @staticmethod
    @st.cache_data
    def read_file_from_snowflake_git_repo(_conn, repo_name, file_path, branch):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir).expanduser().resolve()
            with _conn.cursor() as cur:
                get_command = f"GET @{repo_name}/branches/{branch}/{file_path} file://{local_path.as_posix()};"
                cur.execute(get_command)
                result = cur.fetchone()
                if not (result and result[0]):
                    raise FileNotFoundError(f"File not found in Snowflake stage: {file_path}")
                with open(local_path / result[0], "r", encoding="utf-8") as f:
                    return f.read()

    @staticmethod
    def generate_response_with_cortex(conn, file_content, question, model_name):
        prompt = f"Based on the following file content, please answer the user's question.\n\n--- FILE CONTENT ---\n{file_content}\n--- END FILE CONTENT ---\n\nQuestion: {question}"
        cortex_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s);"
        with conn.cursor() as cur:
            cur.execute(cortex_sql, (model_name, prompt))
            result = cur.fetchone()
            return result[0] if result and result[0] else "Cortex did not return a valid response."




    class GitPublisher:
        def __init__(self, repo_path, remote_url, branch_name, status_callback):
            self.repo_path = repo_path
            self.remote_url = remote_url
            self.branch_name = branch_name
            self.status_callback = status_callback

        def run_command(self, command, can_fail=False):
            self.status_callback(f"▶️ Running: {' '.join(command)}", "info")
            try:
                result = subprocess.run(command, cwd=self.repo_path, check=True, capture_output=True, text=True, timeout=120)
                if result.stdout: self.status_callback(result.stdout, "code")
                return True, result.stdout.strip()
            except subprocess.CalledProcessError as e:
                if not can_fail:
                    self.status_callback(f"❌ Error executing command:\n{e.stderr}", "error")
                return False, e.stderr.strip()

        def _setup_repository(self):
            if not self.run_command(["git", "init"])[0]: return False
            success, current_url = self.run_command(["git", "remote", "get-url", "origin"], can_fail=True)
            if not success:
                self.status_callback("Remote 'origin' not found. Adding it.", "info")
                if not self.run_command(["git", "remote", "add", "origin", self.remote_url])[0]: return False
            elif current_url != self.remote_url:
                self.status_callback("Remote 'origin' points to an incorrect URL. Updating it.", "info")
                if not self.run_command(["git", "remote", "set-url", "origin", self.remote_url])[0]: return False
            else:
                self.status_callback("✅ Remote 'origin' is correctly configured.", "info")
            if not self.run_command(["git", "branch", "-M", self.branch_name])[0]: return False
            return True

        def git_publish_all(self):
            if not self._setup_repository(): return False
            if not self.run_command(["git", "add", "."])[0]: return False
            _, status_output = self.run_command(["git", "status", "--porcelain"])
            if not status_output:
                self.status_callback("✅ No new changes to commit. Nothing to publish.", "success")
                return True
            commit_message = f"Automated batch update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            if not self.run_command(["git", "commit", "-m", commit_message])[0]: return False
            if not self.run_command(["git", "push", "-u", "origin", self.branch_name, "--force"])[0]: return False
            return True

        def git_publish_single_file(self, filename: str):
            if not self._setup_repository(): return False
            file_to_add = Path(self.repo_path) / filename
            if not file_to_add.exists():
                self.status_callback(f"Error: File to publish does not exist: {filename}", "error")
                return False
            if not self.run_command(["git", "add", filename])[0]: return False
            _, status_output = self.run_command(["git", "status", "--porcelain"])
            if not status_output:
                self.status_callback(f"✅ No changes to commit for {filename}. File is up-to-date.", "success")
                return True
            commit_message = f"Automated edit: Update {filename} on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            if not self.run_command(["git", "commit", "-m", commit_message])[0]: return False
            if not self.run_command(["git", "push", "-u", "origin", self.branch_name])[0]: return False
            return True




    @staticmethod
    def fetch_snowflake_git_repo(conn, repo_name):
        with conn.cursor() as cur:
            sql_command = f"ALTER GIT REPOSITORY {repo_name} FETCH;"
            cur.execute(sql_command)
            return cur.fetchone()[0]

    def get_config(self, conversion_type):
        return self.CONVERSION_CONFIG[conversion_type]

    @staticmethod
    def reset_editor_state():
        """Clears editor state when switching conversion types for a clean UI."""
        st.session_state.edit_mode = False
        st.session_state.original_content = ""
        st.session_state.converted_content = ""
        st.session_state.current_file_path = None
        if "file_selector" in st.session_state:
            st.session_state.file_selector = "-- Select a file --"

    def run(self, config:dict):
        # Place the main Streamlit UI logic here, or call this from your main script.
        st.sidebar.title("Conversion Workflow")
        conversion_choice = st.sidebar.radio(
            "Select the script type to convert:",
            ("HQL", "BTEQ"),
            captions=["Hive QL Scripts Conversion", "BTEQ Scripts Conversion"],
            key="conversion_type_selector",
            on_change=self.reset_editor_state
        )
        
        # Load the active configuration based on the user's choice in the sidebar
        # CONFIG = CONVERSION_CONFIG[conversion_choice] 
        CONFIG = self.CONVERSION_CONFIG[st.session_state.conversion_type_selector]
        
        
        # ==============================================================================
        # UI Configuration and Page Setup
        # ==============================================================================
        
        if 'conversion_type_selector' not in st.session_state:
            st.session_state.conversion_type_selector = "HQL" 
        
        
        st.title(f"❄️ {CONFIG['title']}")
        st.markdown(f"""
        {CONFIG['description']}
        1.  **Connects** to a source Snowflake Git repository to find {CONFIG['find_str']} files.
        2.  **Converts** each file to SQL using a Cortex LLM.
        3.  **Publishes** the converted SQL files to a destination GitHub repository.
        4.  **Syncs** the destination GitHub repository back to Snowflake.
        """)
        
        
        # --- Set constants dynamically based on the selected configuration ---
        SNOWFLAKE_REPO_NAME = CONFIG["source_repo"]
        GIT_BRANCH = "main"
        OUTPUT_DIRECTORY = Path(CONFIG["output_dir"])
        GITHUB_REMOTE_URL = CONFIG["github_url"]
        SNOWFLAKE_DEST_REPO_NAME = CONFIG["dest_repo"]
        
        # Cortex Model (common for both workflows)
        CORTEX_MODEL = "snowflake-llama-3.3-70b"
        
        if 'log' not in st.session_state:
            st.session_state.log = []
        if 'edit_mode' not in st.session_state:
            st.session_state.edit_mode = False
        if 'original_content' not in st.session_state:
            st.session_state.original_content = ""
        if 'converted_content' not in st.session_state:
            st.session_state.converted_content = ""
        if 'current_file_path' not in st.session_state:
            st.session_state.current_file_path = None
        if 'active_tab' not in st.session_state:
            st.session_state.active_tab = "Workflow Runner"
        
        
        st.radio(
            "Select a view:",
            ["Workflow Runner", "Editor & Publisher"],
            key="active_tab",
            horizontal=True,
            label_visibility="collapsed"
        )
        
        # ==============================================================================
        # TAB 1: WORKFLOW RUNNER
        # ==============================================================================
        if st.session_state.active_tab == "Workflow Runner":
            st.header("Step 1: Run Batch Conversion")
            st.markdown(f"This will fetch all {CONFIG['find_str']} scripts from the source repo, convert them using Cortex, and save them locally. You can then review them in the 'Editor & Publisher' tab.")
        
            start_button = st.button("🚀 Start Full Conversion Workflow", type="primary", width='stretch')
        
            status_placeholder = st.empty()
            progress_placeholder = st.empty()
            results_expander = st.expander("📊 Conversion Results Summary", expanded=True)
        
            if start_button:
                sf_config = config.get('SNOWFLAKE_CONFIG')
                source_repo_name = SNOWFLAKE_REPO_NAME
                conn = self.get_snowflake_connection(sf_config)
                results = []
                if conn:
                    with st.spinner("Running conversion..."):
                        files_to_process = self.list_files_in_git_repo(conn, source_repo_name, GIT_BRANCH)
                        total_files = len(files_to_process)
                        output_dir = OUTPUT_DIRECTORY
                        output_dir.mkdir(exist_ok=True)
                        files_processed_successfully = 0
        
                        try:
                            with open(CONFIG["prompt_file"], "r", encoding="utf-8") as f:
                                master_prompt = f.read().strip()
                        except FileNotFoundError:
                            st.error(f"Error: Prompt file '{CONFIG['prompt_file']}' not found. Please create it and restart.")
                            st.stop()
        
                        for i, file_path in enumerate(files_to_process):
                            status_placeholder.info(f"🔄 ({i+1}/{total_files}) Converting: {file_path}")
                            try:
                                content = self.read_file_from_snowflake_git_repo(conn, source_repo_name, file_path, GIT_BRANCH)
                               ### --- CHANGE START: Conditional Cortex vs Dummy Logic --- ###
                                if st.session_state.conversion_type_selector == "BTEQ":
                                    # BTEQ is selected: Run dummy process
                                    time.sleep(2)  # Simulate a delay for the spinner
        
                                else:
                                    # HQL (or any other type) is selected: Run actual Cortex call
                                    cortex_response = self.generate_response_with_cortex(conn, content, master_prompt, CORTEX_MODEL)
                                ### --- CHANGE END --- ###
        
        
                                # cortex_response = generate_response_with_cortex(conn, content, master_prompt, CORTEX_MODEL)
                                new_filename = f"converted_{Path(file_path).stem}.sql"
                                if st.session_state.conversion_type_selector == "BTEQ":
                                    pass    
                                else:
                                    with open(output_dir / new_filename, "w", encoding="utf-8") as f: f.write(cortex_response)
        
                                results.append({"File": file_path, "Status": "✅ Success", "Details": f"Saved as {new_filename}"})
                                files_processed_successfully += 1
                            except Exception as e:
                                results.append({"File": file_path, "Status": "❌ Failed", "Details": str(e)})
                            progress_placeholder.progress((i + 1) / total_files)
        
                    status_placeholder.success("✅ Conversion complete! Review files in the 'Editor & Publisher' tab.")
                    log_info(f"\n--- PHASE 1 COMPLETE: {files_processed_successfully}/{len(files_to_process)} files converted. ---")
                    results_expander.dataframe(pd.DataFrame(results), width='stretch')
        
                    if files_processed_successfully > 0:
                        with st.spinner("Publishing all converted files to GitHub..."):
                            log_messages = []
                            def git_status_callback(message, level): log_messages.append(message)
                            publisher = self.GitPublisher(str(output_dir.resolve()), GITHUB_REMOTE_URL, GIT_BRANCH, git_status_callback)
                            publish_success = publisher.git_publish_all()
        
                            if publish_success:
                                st.success("✅ GitHub publish complete!")
                                with st.spinner("Syncing Snowflake repository..."):
                                    log_messages.append("\n--- Syncing Snowflake ---")
                                    sync_message = self.fetch_snowflake_git_repo(conn, SNOWFLAKE_DEST_REPO_NAME)
                                    log_messages.append(f"✅ {sync_message}")
                                    st.success("🎉 Full workflow complete!")
                            else:
                                st.error("❌ Publishing to GitHub failed.")
                            results_expander.code("\n".join(log_messages), language="log")
        
        # ==============================================================================
        # TAB 2: EDITOR & PUBLISHER
        # ==============================================================================
        elif st.session_state.active_tab == "Editor & Publisher":
            st.header("Step 2: Compare, Edit, and Publish")
            st.markdown("Select a converted script to compare it with the original. You can edit the converted script and then publish individual changes to GitHub and Snowflake.")
        
            def load_file_content():
                selected_file = st.session_state.get("file_selector")
                st.session_state.edit_mode = False
        
                if selected_file and selected_file != "-- Select a file --":
                    converted_path = OUTPUT_DIRECTORY / selected_file
                    st.session_state.current_file_path = str(converted_path)
                    st.session_state.converted_content = converted_path.read_text(encoding="utf-8")
        
                    # Dynamically deduce original filename based on selected conversion type
                    # original_stem = selected_file.replace("converted_", "").replace(".sql", "")
                    original_stem = (
                        selected_file.replace("converted_", "")
                                     .replace(".sql", "")
                                     .replace(".py", "")
                    )
        
        
                    source_extension = CONFIG["source_extension"]
                    original_filename = f"{original_stem}{source_extension}"
        
                    # sf_config = SNOWFLAKE_CONFIG
                    sf_config = config.get('SNOWFLAKE_CONFIG')
                    conn = self.get_snowflake_connection(sf_config)
                    if conn:
                        try:
                            st.session_state.original_content = self.read_file_from_snowflake_git_repo(conn, SNOWFLAKE_REPO_NAME, original_filename, GIT_BRANCH)
                        except Exception as e:
                            st.session_state.original_content = f"--- ERROR: Could not fetch original file '{original_filename}' ---\n{e}"
                else:
                    st.session_state.original_content = ""
                    st.session_state.converted_content = ""
                    st.session_state.current_file_path = None
        
            if OUTPUT_DIRECTORY.exists():
                # converted_files = sorted([f.name for f in OUTPUT_DIRECTORY.glob("*.sql")])
                converted_files = sorted(
                        [f.name for f in OUTPUT_DIRECTORY.glob("*.sql")] +
                        [f.name for f in OUTPUT_DIRECTORY.glob("*.py")]
                    )
        
                options = ["-- Select a file --"] + converted_files
                st.selectbox("Choose a converted script to review:", options, key="file_selector", on_change=load_file_content)
            else:
                st.info(f"No converted scripts found in '{OUTPUT_DIRECTORY}/'. Run the workflow in the 'Workflow Runner' tab first.")
        
            st.divider()
        
            if st.session_state.current_file_path:
                comp_col1, comp_col2 = st.columns(2)
                with comp_col1:
                    st.markdown(f"**Original {CONFIG['find_str']} Script**")
                    with st.container(height=600): st.code(st.session_state.original_content, language='sql', line_numbers=True)
        
                with comp_col2:
                    st.markdown("**Converted SQL Script (Editable)**")
        
                    btn_cols = st.columns(2)
                    with btn_cols[0]:
                        if not st.session_state.edit_mode:
                            if st.button("✏️ Edit Script", width='stretch'):
                                st.session_state.edit_mode = True
                                st.rerun()
                        else: # Show Save button in the same spot
                            if st.button("💾 Save Changes", width='stretch', type="primary"):
                                new_content = st.session_state.editor_content
                                Path(st.session_state.current_file_path).write_text(new_content, encoding='utf-8')
                                st.session_state.converted_content = new_content
                                st.session_state.edit_mode = False
                                st.toast("✅ File saved!", icon="💾")
                                st.rerun()
        
                    with btn_cols[1]:
                        if not st.session_state.edit_mode:
                             if st.button("🚀 Publish This File", width='stretch'):
                                with st.spinner("Publishing changes for this file..."):
                                    log_placeholder = st.expander("Publisher Log", expanded=True)
                                    log_messages = []
                                    def git_status_callback(message, level): log_messages.append(message)
        
                                    publisher = self.GitPublisher(str(OUTPUT_DIRECTORY.resolve()), GITHUB_REMOTE_URL, GIT_BRANCH, git_status_callback)
                                    filename_to_publish = Path(st.session_state.current_file_path).name
                                    publish_success = publisher.git_publish_single_file(filename_to_publish)
        
                                    if publish_success:
                                        # sf_config = SNOWFLAKE_CONFIG
                                        sf_config = config.get('SNOWFLAKE_CONFIG')
                                        conn = self.get_snowflake_connection(sf_config)
                                        if conn:
                                            log_messages.append("\n--- Syncing Snowflake ---")
                                            sync_message = self.fetch_snowflake_git_repo(conn, SNOWFLAKE_DEST_REPO_NAME)
                                            log_messages.append(f"✅ {sync_message}")
                                            st.success(f"🎉 Successfully published and synced {filename_to_publish}!")
                                    else:
                                        st.error("❌ Publishing failed.")
                                    log_placeholder.code("\n".join(log_messages))
        
                    if st.session_state.edit_mode:
                        st.text_area("Edit the script below:", value=st.session_state.converted_content, height=530, key="editor_content", label_visibility="collapsed")
                    else:
                        with st.container(height=580):
                            st.code(st.session_state.converted_content, language='sql', line_numbers=True)
        

