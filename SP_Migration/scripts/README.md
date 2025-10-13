### The `scripts/` Directory: The Core Engine

This directory contains the heart of the application's functionality, with a clear separation between UI-facing modules and backend logic.

#### UI Component Modules (`*_st.py` / `*_tests.py`)
These modules are responsible for rendering the Streamlit UI for a specific step in the workflow. They are called by `app.py`.
-   **`update_flag_st.py`**: Renders the interactive UI for **Step 2**. It displays procedures in color-coded, searchable, scrollable expanders and handles the logic for setting conversion flags and extracting source code.
-   **`convert_scripts_st.py`**: Renders the UI for **Step 3**. It provides the button to run the SnowConvert process, shows real-time logs, and displays the conversion analytics dashboard. It also manages the Azure cache and Git publishing actions.
-   **`process_procs_st.py`**: Renders the UI for **Step 4**. This is the most complex UI component, featuring the side-by-side file comparator, the toggle-able code editor, and the button to trigger a unit test for a single procedure.
-   **`run_py_tests.py`**: Renders the UI for **Step 5**. It contains the buttons to execute the bulk test suite and to refresh the results. It also renders the filterable dashboard with metrics and a styled DataFrame of the test outcomes.

#### Backend Logic & Utility Modules
These modules contain the "headless" Python code that performs the actual work. They are called by the UI modules or by `app.py`.
-   **`create_metadata_table.py`**: (**Step 1 Backend**) Contains the `CreateMetadataTable` class. Its methods connect to SQL Server, query the `INFORMATION_SCHEMA`, and use a `MERGE` statement to idempotently insert or update procedure metadata in the Snowflake tracking table.
-   **`extract_procedures.py`**: (**Step 2 Backend**) Connects to Snowflake, queries the metadata table for procedures where `CONVERSION_FLAG` is true, and writes their source definitions to `.sql` files in the `./extracted_procedures` directory.
-   **`convert_scripts.py`**: (**Step 3 Backend**) A robust Python wrapper around the `snowct` command-line tool. It handles checking for its existence, setting up the license, and executing the conversion command with the correct input and output paths.
-   **`process_sc_script.py`**: (**Step 4 Backend**) The `ScScriptProcessor` class performs automated cleanup on the converted files. It contains regex-based logic to remove comments, replace schema names, and apply other necessary transformations.
-   **`py_test.py`**: (**Step 5 Backend**) The core testing engine. It defines a `unittest.TestCase` class (`TestStoredProcedure`) with methods to test procedure creation (`test_create_procedure_from_file`) and execution (`test_procedure_execution`). It also includes the `run_single_test` function, a crucial component that allows for the isolated testing of a single file from the UI.
-   **`py_output.py`**: A simple utility class that connects to Snowflake and executes a `SELECT *` query on the `TEST_RESULTS_LOG` table, returning the data for display in the UI dashboard.
-   **`git_publisher.py`**: A utility class that encapsulates all Git logic. It handles staging files, committing with a dynamic message, and pushing to the remote repository. It is designed to operate directly on the project's root Git repository.
-   **`log.py`**: A standard Python logging setup utility. It configures a logger to write to both the console and the persistent `logs/Sp_convertion.log` file, ensuring all backend actions are recorded.