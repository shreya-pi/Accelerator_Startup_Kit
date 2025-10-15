# --- START OF FILE run_unit_tests_st.py ---

import streamlit as st
import pandas as pd
import os
import unittest
import importlib
import io
import shutil

class UnitTestPage:
    def __init__(self, config: dict):
        """
        Initializes the unit testing page with the application configuration.
        """
        if not config:
            raise ValueError("Configuration is required to run the unit testing page.")
        self.config = config
        
        # Initialize session state for this page if not already present
        if 'test_results_df' not in st.session_state: 
            st.session_state.test_results_df = None
        if 'step_completion' not in st.session_state:
            st.session_state.step_completion = {
                'run_unit_tests': False
            }

    def display_page(self):
        """
        Renders the entire UI for the unit testing component.
        """
        # --- 1. INTRODUCTION CONTAINER ---
        with st.container(border=True):
            st.subheader("🏁 Unit Testing Workflow")
            st.markdown(
                """
                This component executes unit tests against your processed procedures and displays the results.
                - **Step 1:** Executes all tests found in `py_test.py`. Results are logged to the `TEST_RESULTS_LOG` table in Snowflake.
                - **Step 2:** Fetches and displays the results from the log table, allowing you to filter and analyze the outcome.
                """
            )

        st.markdown("---")

        # --- 2. ACTION CONTAINER ---
        with st.container(border=True):
            st.subheader("⚙️ Actions")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Step 1: Execute Tests")
                if st.button("🚀 **Execute All Unit Tests**", use_container_width=True, help="Runs the full test suite and populates the log table in Snowflake."):
                    self.run_tests()

            with col2:
                st.markdown("#### Step 2: View Results")
                if st.button("📊 **View/Refresh Test Results**", use_container_width=True, help="Fetches the latest data from the log table to display in the dashboard below."):
                    self.fetch_results()

        st.markdown("---")

        # --- 3. DASHBOARD CONTAINER ---
        with st.container(border=True):
            st.subheader("📋 Test Results Dashboard")
            if st.session_state.test_results_df is None:
                st.info("ℹ️ Click 'View/Refresh Test Results' to load data from the Snowflake log table.")
            elif st.session_state.test_results_df.empty:
                st.warning("Query executed, but no test results were found in the `TEST_RESULTS_LOG` table.")
            else:
                self.display_dashboard()

        if st.session_state.test_results_df is not None and not st.session_state.test_results_df.empty:
            st.markdown("---")
            with st.container(border=True):
                st.subheader("🚀 Publish Deployed Procedures to GitHub")
                st.markdown(
                    """
                    This action will find all procedures that **passed the `test_procedure_execution` test**,
                    copy them to a `deployed_procedures` folder, and push them to your configured Git repository.
                    """
                )
                if st.button("🐙 **Publish to Git**", use_container_width=True, type="primary"):
                    self.publish_to_git()


    def publish_to_git(self):
        """
        Filters results, copies successful files, and calls the Git utility function.
        """
        try:
            with st.spinner("Preparing files for publishing..."):
                df = st.session_state.test_results_df
                
                # 1. Filter for successfully executed procedures
                deployed_procs_df = df[
                    (df['STATUS'] == '✅ Success') & 
                    (df['TEST_CASE_NAME'] == 'test_procedure_execution')
                ]

                if deployed_procs_df.empty:
                    st.warning("No successfully executed procedures found to publish.")
                    st.stop()
                
                successful_proc_names = set(deployed_procs_df['PROCEDURE_NAME'])
                
                # 2. Prepare directories
                source_dir = "./SP_Migration/processed_procedures"
                target_dir = "./SP_Migration/deployed_procedures"
                github_url = "https://github.com/shreya-pi/SP_Deployed_Procs_Test.git"
                # if not os.path.exists(parent_target_dir):
                #     os.makedirs(parent_target_dir)
                # target_dir = "./deployed_procedures/deployed_procedures"
                # 1. Ensure the directory exists.
                os.makedirs(target_dir, exist_ok=True)

                # 2. Clean out OLD files from the directory, but DO NOT touch the .git folder.
                # This prevents the file lock PermissionError.

                # st.write("Cleaning previous deployment artifacts...")
                for item in os.listdir(target_dir):
                    item_path = os.path.join(target_dir, item)
                    # The crucial check: leave the .git directory alone.
                    if item != ".git":
                        try:
                            if os.path.isfile(item_path) or os.path.islink(item_path):
                                os.unlink(item_path)
                            elif os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                        except Exception as e:
                            st.error(f"Failed to delete {item_path}. Reason: {e}")
                            st.stop()


               # 3. Copy the relevant files
                # This logic assumes the filename in processed_procedures matches the PROCEDURE_NAME from the log.
                # If not, you may need a more complex mapping based on the regex in py_test.py.
                copied_files = 0
                for proc_name in successful_proc_names:
                    # Construct the expected source filename. Adjust if your naming is different.
                    source_file = f"processed_{proc_name}.sql"
                    target_file_name = f"deployed_{proc_name}.sql" 
                    source_path = os.path.join(source_dir, source_file)
                    
                    if os.path.exists(source_path):
                        target_path = os.path.join(target_dir, target_file_name)
                        shutil.copy(source_path, target_path)
                        copied_files += 1
                    else:
                        st.warning(f"Could not find source file for procedure '{proc_name}' at '{source_path}'. Skipping.")
                
                if copied_files == 0:
                    st.error("Found successful procedures in log, but could not find any corresponding files in `processed_procedures`.")
                    st.stop()

                st.write(f"Found and copied {copied_files} deployed procedure files.")

            # 4. Call the Git publishing function
            with st.spinner("Connecting to Git and publishing files..."):
                from SP_Migration.scripts.git_publisher import GitPublisher
                git_publisher = GitPublisher(target_dir, github_url)
                git_publisher.git_publish()
            
            st.success(f"✅ Git Publish Completed!")

        except Exception as e:
            st.error("❌ An error occurred during the publish process:")
            st.exception(e)




    def run_tests(self):
        """
        Handles the logic for executing the unittest suite.
        """
        with st.spinner("Executing unit tests... This may take a moment."):
            try:
                from SP_Migration.scripts import py_test
                importlib.reload(py_test)
                
                # Pass the config to the test module
                py_test.CONFIG = self.config

                processed_dir = "./SP_Migration/processed_procedures"
                if not os.path.exists(processed_dir):
                    st.error(f"Directory '{processed_dir}' not found. Please run Step 5 first."); st.stop()
                
                sql_files = [f for f in os.listdir(processed_dir) if f.endswith(".sql")]
                if not sql_files:
                    st.warning("No processed SQL files found to test."); st.stop()

                st.write(f"Found {len(sql_files)} procedures to test...")
                progress_bar = st.progress(0, text="Initializing tests...")
                
                loader = unittest.TestLoader()
                # Capture output to prevent cluttering the UI
                runner = unittest.TextTestRunner(stream=io.StringIO()) 

                for i, file_name in enumerate(sql_files):
                    progress_bar.progress((i + 1) / len(sql_files), text=f"Testing: {file_name}")
                    suite = loader.loadTestsFromTestCase(py_test.TestStoredProcedure)
                    # Dynamically assign the sql_file to each test instance
                    for test in suite:
                        test.sql_file = os.path.join(processed_dir, file_name)
                    runner.run(suite)
                
                st.success("✅ All test execution cycles complete. **Click 'View/Refresh Test Results'** to see the outcome.")
                
                st.session_state.test_results_df = None  # Force a refresh on next view
            except Exception as e:
                st.error("❌ An error occurred during unit test execution:"); st.exception(e)




    def fetch_results(self):
        """
        Handles the logic for fetching test results from Snowflake.
        """
        with st.spinner("Fetching results from Snowflake log table..."):
            try:
                from SP_Migration.scripts.py_output import PyOutput
                output_viewer = PyOutput(config=self.config) 
                results_data, column_names = output_viewer.display_PyOutput()
                
                if results_data:
                    df = pd.DataFrame(results_data, columns=column_names)
                    st.session_state.test_results_df = df
                else:
                    st.session_state.test_results_df = pd.DataFrame(columns=column_names or [])
                
                st.session_state.step_completion['run_unit_tests'] = True  # Mark this step as completed
                st.success("✅ Results fetched successfully. You can now filter and analyze the test results.")
                st.rerun() # Rerun to update the dashboard display
            except Exception as e:
                st.error("❌ An error occurred while fetching results:"); st.exception(e)
                st.session_state.test_results_df = None




    def display_dashboard(self):
        """
        Renders the filters, metrics, and data table for the test results.
        """
        df = st.session_state.test_results_df

        st.markdown("#### Filter Results")
        filter_cols = st.columns(4)
        proc_options = sorted(df['PROCEDURE_NAME'].unique())
        status_options = sorted(df['STATUS'].unique())
        id_options = sorted(df['TEST_CASE_ID'].unique()) if 'TEST_CASE_ID' in df.columns else []
        name_options = sorted(df['TEST_CASE_NAME'].unique()) if 'TEST_CASE_NAME' in df.columns else []

        selected_procs = filter_cols[0].multiselect("PROCEDURE_NAME", proc_options)
        selected_status = filter_cols[1].multiselect("STATUS", status_options)
        selected_ids = filter_cols[2].multiselect("TEST_CASE_ID", id_options)
        selected_names = filter_cols[3].multiselect("TEST_CASE_NAME", name_options)

        filtered_df = df.copy()
        if selected_procs: filtered_df = filtered_df[filtered_df['PROCEDURE_NAME'].isin(selected_procs)]
        if selected_status: filtered_df = filtered_df[filtered_df['STATUS'].isin(selected_status)]
        if selected_ids:
                filtered_df = filtered_df[filtered_df['TEST_CASE_ID'].isin(selected_ids)]
        if selected_names:
                filtered_df = filtered_df[filtered_df['TEST_CASE_NAME'].isin(selected_names)]

        total_tests = len(filtered_df)
        passed_tests = len(filtered_df[filtered_df['STATUS'] == '✅ Success'])
        failed_tests = total_tests - passed_tests

        st.markdown("#### Summary of Filtered Data")
        metric_cols = st.columns(3)
        metric_cols[0].metric("Total Results Shown", total_tests)
        metric_cols[1].metric("Passed ✅", passed_tests)
        metric_cols[2].metric("Failed ❌", failed_tests, delta_color="inverse" if failed_tests > 0 else "off")

        def style_results(df_to_style):
            def highlight(row):
                if row.get('STATUS') == '✅ Success': return ['background-color: #d4edda; color: #155724'] * len(row)
                if row.get('STATUS') == '❌ Failed': return ['background-color: #f8d7da; color: #721c24'] * len(row)
                return [''] * len(row)
            return df_to_style.style.apply(highlight, axis=1)
        
        st.dataframe(style_results(filtered_df), use_container_width=True)



