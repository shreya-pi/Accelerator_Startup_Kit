import os
import subprocess
import streamlit as st
import sys
import importlib
import unittest
import re
import pandas as pd
import uuid
from dotenv import load_dotenv
from datetime import datetime, timedelta
import streamlit_authenticator as stauth
import yaml
import uuid
# from yaml.loader import SafeLoader
import pyodbc
# Database imports
import sqlalchemy as sqla
from sqlalchemy.exc import IntegrityError


from SP_Migration.scripts.create_metadata_table import CreateMetadataTable
from SP_Migration.scripts.update_flag_st import SelectProcedures
from SP_Migration.scripts.convert_scripts_st import ConvertPage
from SP_Migration.scripts.process_procs_st import ProcessProcsPage
from SP_Migration.scripts.run_py_tests import UnitTestPage

load_dotenv() 
DATABASE_URL = os.environ.get("DATABASE_URL")

# Load environment variables for Azure Storage connection

class SPMigrationApp:
    def __init__(self):
        self.component_titles = {
            "create_metadata": "1. Load Procedures from Source",
            "update_flag": "2. Select, Flag & Extract Procedures",
            "convert_procs": "3. Convert Procedures with SnowConvert",
            "process_converted_procs": "4. Process & Finalize Scripts",
            "run_unit_tests": "5. Execute Unit Tests & Review Results"
        }
        self.sidebar_labels = {
            "create_metadata": "1. Load Procedures from Source",
            "update_flag": "2. Choose Procedures to Migrate",
            "convert_procs": "3. Convert Procedures to SnowScript",
            "process_converted_procs": "4. Process Scripts",
            "run_unit_tests": "5. Run Unit Tests"
        }

    def initialize_session_state(self):
        if "active_component" not in st.session_state:
            st.session_state.active_component = "create_metadata"
        if "config_py" not in st.session_state:
            st.session_state.config_py = None
        if "app_config" not in st.session_state:
            st.session_state.app_config = None
        if "show_analytics" not in st.session_state:
            st.session_state.show_analytics = False
        if "step_completion" not in st.session_state:
            st.session_state.step_completion = {key: False for key in self.component_titles}
        if 'test_results_df' not in st.session_state:
            st.session_state.test_results_df = None
        if 'source_schema' not in st.session_state:
            st.session_state.source_schema = ""
        if 'target_schema' not in st.session_state:
            st.session_state.target_schema = ""
        if "log_messages" not in st.session_state:
            st.session_state.log_messages = []
        if "last_action_status" not in st.session_state:
            st.session_state.last_action_status = None

    def display_log_viewer(self):
        st.sidebar.markdown("---")
        st.sidebar.subheader("Activity Log")
        log_file_path = os.path.join("logs", "Sp_convertion.log")
        log_content = ""
        if os.path.exists(log_file_path):
            try:
                with open(log_file_path, "r", encoding="utf-8", errors="replace") as f:
                    log_content = f.read()
            except Exception as e:
                st.sidebar.error(f"Error reading log file: {e}")
        st.sidebar.download_button(
            label="📥 Download Full Log",
            data=log_content,
            file_name="Sp_convertion.log",
            mime="text/plain",
            use_container_width=True,
            disabled=not log_content
        )
        with st.sidebar.expander("View Live Log (Last 25 Lines)"):
            if log_content:
                log_lines = log_content.strip().split('\n')
                st.code('\n'.join(log_lines[-25:]), language='log')
            else:
                st.info("Log file is empty or has not been created yet.")
            if st.button("🔄 Refresh Log", use_container_width=True):
                st.rerun()

    def process_action_flags(self):
        action = st.session_state.last_action_status
        if action:
            if action == 'create_metadata_success':
                st.session_state.step_completion['create_metadata'] = True
            elif action == 'update_flag_success':
                st.session_state.step_completion['update_flag'] = True
            elif action == 'convert_procs_success':
                st.session_state.step_completion['convert_procs'] = True
            elif action == 'process_procs_success':
                st.session_state.step_completion['process_converted_procs'] = True
            elif action == 'run_tests_success':
                st.session_state.step_completion['run_unit_tests'] = True
            st.session_state.last_action_status = None

    def get_db_engine(self):
        """Creates a SQLAlchemy engine. Returns None on failure."""
        if not DATABASE_URL:
            st.error("Database URL is not configured. Please contact an administrator.")
            return None
        try:
            return sqla.create_engine(DATABASE_URL)
        except Exception as e:
            st.error(f"Failed to connect to the database: {e}")
            return None
    
    def fetch_credentials_from_db(self,engine):
        """Fetches user data from the DB and formats it for the authenticator."""
        credentials = {'usernames': {}}
        if engine is None:
            return credentials
    
        with engine.connect() as conn:
            result = conn.execute(sqla.text("SELECT username, name, email, password_hash FROM users"))
            for row in result:
                user_data = {
                    'name': row.name,
                    'email': row.email,
                    'password': row.password_hash
                }
                # Add the user data to the credentials dictionary
                credentials['usernames'][row.username] = user_data  
    
        return credentials

    def write_new_user_to_db(self,engine, username, name, email, hashed_password):
        """Writes a new user's details to the database."""
        if engine is None:
            return False, "Database connection failed."
        
        insert_statement = sqla.text("""
            INSERT INTO users (username, name, email, password_hash)
            VALUES (:username, :name, :email, :password_hash)
        """)
        try:
            with engine.connect() as conn:
                conn.execute(insert_statement, {
                    "username": username,
                    "name": name,
                    "email": email,
                    "password_hash": hashed_password
                })
                conn.commit()
            # st.session_state.user_id= username
            return True, "User registered successfully!"
        except IntegrityError:
            return False, "This username already exists."
        except Exception as e:
            return False, f"An error occurred: {e}"

    def run(self, config:dict):



        # --- Authentication Setup ---
        db_engine = self.get_db_engine()
        credentials = self.fetch_credentials_from_db(db_engine)
        
        authenticator = stauth.Authenticate(
            credentials,
            "streamlit_auth_cookie", # cookie_name
            "some_random_secret_key_change_it", # cookie_key
            30 # cookie_expiry_days
        )
        
        # --- Main App ---
        # st.set_page_config(layout="wide")
        
        # Show Login or Register options
        # Using st.tabs for a clean UI to switch between Login and Register
        login_tab, register_tab = st.tabs(["Login", "Register"])
        
        with login_tab:
            # st.image("assets/Tulapi logo.png", width=150) 
            st.subheader("Login to Your Account")
            authenticator.login()
        
            if st.session_state["authentication_status"]:
                # --- Logged-in App ---
                if 'user_id' not in st.session_state:
                    st.session_state.user_id = st.session_state["username"]
                name = st.session_state["name"]
        
                #       # --- SIDEBAR ---
                with st.sidebar:
                    st.image("SP_Migration/assets/Tulapi_logo.png", width=150) # Adjust width as needed
                    
                    # The rest of your sidebar code follows
                    st.write(f'Welcome *{name}*')
                    authenticator.logout('Logout', 'sidebar') # Moved logout below welcome message
            
                username = st.session_state["username"]
                # st.sidebar.write(f'Welcome *{name}*')
                # authenticator.logout('Logout', 'sidebar')        
                # Add a visual separator below the header
                # st.markdown("---")
                # --- Main Application Logic ---        
        
        # st.set_page_config(
        #     page_title="SP Migration Assistant",
        #     layout="wide",
        #     initial_sidebar_state="expanded",
        #     menu_items=None
        # )
                st.markdown("---")
                self.initialize_session_state()
                self.process_action_flags()
                with st.sidebar:
                    st.header("SP Migration Assistant")
                    st.markdown("Follow the workflow to migrate your stored procedures.")
                    st.markdown("---")
                    for key, label in self.sidebar_labels.items():
                        is_completed = st.session_state.step_completion.get(key, False)
                        is_active = (st.session_state.active_component == key)
                        status_icon = "✅" if is_completed else "➡️" if is_active else "⏳"
                        button_label = f"{status_icon} {label}"
                        if st.button(button_label, use_container_width=True, type="secondary" if not is_active else "primary"):
                            st.session_state.active_component = key
                    st.markdown("---")
                    completed_count = sum(1 for status in st.session_state.step_completion.values() if status)
                    total_steps = len(self.component_titles)
                    progress = completed_count / total_steps
                    st.progress(progress, text=f"{completed_count} of {total_steps} Steps Complete")
                    with st.expander("Show Session Log"):
                        self.display_log_viewer()
                title_col, logo_col = st.columns([5, 1])
                with title_col:
                    st.title("SQL Server to Snowflake Migration Tool for Stored Procedures")
                main_col = st.container()
                with main_col:
                    active_key = st.session_state.active_component
                    if active_key == "create_metadata":
                        with st.container(border=True):
                            # st.subheader("Configuration")
                            # st.markdown("Upload your `config.py` file to provide the database connection details for the application.")
                            # with st.expander("View Sample `config.py` Template"):
                            #     with open('SP_Migration/assets/config_template.py', 'r') as file:
                            #         config_template = file.read()
                            #     st.code(config_template, language='python')
                            # uploaded = st.file_uploader(label="Upload `config.py`", type=["py"], label_visibility="collapsed")
                            # if uploaded:
                            #     try:
                            #         raw_bytes = uploaded.read()
                            #         config_code = raw_bytes.decode("utf-8")
                            #         ns = {}
                            #         exec(config_code, {}, ns)
                            #         st.session_state.app_config = {
                            #             "SNOWFLAKE_CONFIG": ns["SNOWFLAKE_CONFIG"],
                            #             "SQL_SERVER_CONFIG": ns["SQL_SERVER_CONFIG"]
                            #         }
                            #         st.success("Config loaded successfully. You can now run the initialization.")
                            #     except Exception as e:
                            #         st.error(f"Failed to parse config: {e}")
                            #         st.session_state.app_config = None
                            # st.markdown("---")
                            st.session_state.app_config = config
                            if st.session_state.app_config:
                                st.markdown("---")
                                try:
                                    creator = CreateMetadataTable(
                                        config=st.session_state.app_config
                                    )
                                    creator.create_metadata_table()
                                    st.toast("✅ Metadata table created/updated successfully!")
                                    st.session_state.step_completion['create_metadata'] = True
                                except Exception as e:
                                    st.error(f"Metadata creation failed: {e}")
                    elif active_key == "update_flag":
                        if st.session_state.app_config:
                            try:
                                select_procedures = SelectProcedures(config=st.session_state.app_config)
                                select_procedures.run_update_flag()
                                st.session_state.step_completion['update_flag'] = True
                            except ImportError:
                                st.error("❌ `update_flag_st.py` not found. Make sure it's in the same directory.")
                            except Exception as e:
                                st.error(f"❌ Error running the update flag interface: {e}")
                        else:
                            st.warning("`config.py` has not been uploaded. Please go to '1. Create Metadata Table' to upload it.")
                    elif active_key == "convert_procs":
                        if st.session_state.app_config:
                            try:
                                convert_ui = ConvertPage(config=st.session_state.app_config)
                                convert_ui.display_page()
                            except ImportError:
                                st.error("❌ `convert_scripts_st.py` not found. Make sure it's in the same directory.")
                            except Exception as e:
                                st.error("❌ An error occurred while loading the conversion page:")
                                st.exception(e)
                        else:
                            st.warning("`config.py` has not been uploaded. Please go to '1. Create Metadata Table' to upload it.")
                    elif active_key == "run_unit_tests":
                        if st.session_state.app_config:
                            try:
                                unit_test_ui = UnitTestPage(config=st.session_state.app_config)
                                unit_test_ui.display_page()
                            except ImportError:
                                st.error("❌ `run_unit_tests_st.py` not found. Make sure it's in the same directory.")
                            except Exception as e:
                                st.error("❌ An error occurred while loading the unit test page:")
                                st.exception(e)
                        else:
                            st.warning("`config.py` has not been uploaded. Please go to '1. Create Metadata Table' to upload it.")
                    elif active_key == "process_converted_procs":
                        if st.session_state.app_config:
                            try:
                                process_ui = ProcessProcsPage(config=st.session_state.app_config)
                                process_ui.display_page()
                            except ImportError:
                                st.error("❌ `process_procs_st.py` not found. Make sure it's in the `scripts` directory.")
                            except Exception as e:
                                st.error("❌ An error occurred while loading the 'Process Procedures' page:")
                                st.exception(e)
                        else:
                            st.warning("`config.py` has not been uploaded. Please go to '1. Create Metadata Table' to upload it.")

            elif st.session_state["authentication_status"] is False:
                st.error('Username/password is incorrect') 
            elif st.session_state["authentication_status"] is None:
                st.warning('Please enter your username and password')


        with register_tab:
            st.subheader("Create a New Account")
        
            with st.form("Registration Form"):
                new_name = st.text_input("Name*")
                new_username = st.text_input("Username*")
                new_email = st.text_input("Email*")
                new_password = st.text_input("Password*", type="password")
                new_password_confirm = st.text_input("Confirm Password*", type="password")
        
                submitted = st.form_submit_button("Register")
        
                if submitted:
                    if not all([new_name, new_username, new_email, new_password, new_password_confirm]):
                        st.error("Please fill out all required fields.")
                    elif new_password != new_password_confirm:
                        st.error("Passwords do not match.")
                    else:
                        try:
                            # Hash the password using the updated method
                            hashed_password = stauth.Hasher().hash(new_password)
        
                            # Attempt to write the new user to the database
                            success, message = self.write_new_user_to_db(
                                db_engine, new_username, new_name, new_email, hashed_password
                            )
        
                            if success:
                                st.success(message)
                                st.info("You can now log in using the 'Login' tab.")
                            else:
                                st.error(message)
                        except Exception as e:
                            st.error(f"An error occurred during registration: {e}")



