import snowflake.connector
import unittest
import os
import io
import sys
import re
from datetime import datetime, timezone
# from config import SNOWFLAKE_CONFIG
from SP_Migration.scripts.log import log_info,log_error
import sqlparse


# Create the Snowflake table
# First, run this DDL once in your Snowflake console (or via a migration script):

# CREATE OR REPLACE TABLE TEST_RESULTS_LOG (
#   TEST_CASE_ID     STRING,
#   TEST_CASE_NAME   STRING,
#   PROCEDURE_NAME   STRING,
#   TEST_TIMESTAMP   TIMESTAMP_NTZ,
#   STATUS           STRING,
#   ERRORS           STRING
# );

test_case_id_counter = 0
CONFIG = None

# Global list to store test results
test_results = []

# Snowflake DDL target table
# PYUNIT_OUTPUT_TABLE = "TEST_RESULTS_LOG"
# METADATA_TABLE = "PROCEDURES_METADATA"

def generate_html_report(results, output_file="py_tests/py_results.html"):
    """Generates a dynamic HTML file with test results."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stored Procedure Test Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid black; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .success { color: green; font-weight: bold; }
        .failure { color: red; font-weight: bold; }
        pre { white-space: pre-wrap; word-wrap: break-word; max-height: 200px; overflow-y: auto; }
    </style>
</head>
<body>
    <h2>Stored Procedure Test Report</h2>
    <table>
        <tr>
            <th>Stored Procedure</th>
            <th>Test Name/Type</th>
            <th>Status</th>
            <th>Reason for Failure</th>
        </tr>"""

    for proc_name, test_type, status, reason, output in results:  # Unpacking 4 values
        status_class = "success" if status == "✅ Success" else "failure"
        html_content += f"""
        <tr>
            <td>{proc_name}</td>
            <td>{test_type}</td>
            <td class="{status_class}">{status}</td>
            <td>{reason}</td>
            <td><pre>{output}</pre></td>
        </tr>"""

    html_content += """
    </table>
</body>
</html>"""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)  # Ensure the directory exists

    with open(output_file, "w", encoding="utf-8") as file:
        file.write(html_content)

    log_info(f"Test report generated: {os.path.abspath(output_file)} for {proc_name}")





    # print("Successfully executed all statements in the script.")

    def run_single_test(sql_file_path, config):
        """
        Executes the full test suite from TestStoredProcedure for a SINGLE SQL file.
        This function manages its own setup and teardown of the test environment.
    
        Args:
            sql_file_path (str): The absolute or relative path to the single .sql file to test.
            config (dict): The application configuration dictionary containing SNOWFLAKE_CONFIG.
    
        Returns:
            tuple: A tuple containing (bool: success, str: test_output_log).
        """
        global CONFIG
        CONFIG = config
        
        # Ensure the test class is in a clean state before starting
        TestStoredProcedure.conn = None
        TestStoredProcedure.cursor = None
    
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromTestCase(TestStoredProcedure)
        
        # Check if we actually loaded tests
        if suite.countTestCases() == 0:
            return False, "Failed to load any tests from TestStoredProcedure."
    
        # Inject the target SQL file into every test case instance in the suite
        for test in suite:
            test.sql_file = sql_file_path
    
        # Use an in-memory string buffer to capture the test runner's output
        output_capture = io.StringIO()
        runner = unittest.TextTestRunner(stream=output_capture, verbosity=2)
        
        # Manually manage setup and teardown to ensure connection handling
        try:
            TestStoredProcedure.setUpClass()
            result = runner.run(suite)
        finally:
            # Crucially, always ensure the connection is closed
            TestStoredProcedure.tearDownClass()
    
        # Get the captured output
        test_output = output_capture.getvalue()
        
        # Check if the test run was successful
        success = result.wasSuccessful()
        
        return success, test_output





def run_single_test(sql_file_path, config):
    """
    Executes the full test suite from TestStoredProcedure for a SINGLE SQL file,
    captures the structured results, and returns them.

    Args:
        sql_file_path (str): The absolute or relative path to the single .sql file to test.
        config (dict): The application configuration dictionary containing SNOWFLAKE_CONFIG.

    Returns:
        list: A list of tuples, where each tuple contains the structured result
              of a single test: (proc_name, test_type, status, reason, output)
    """
    global CONFIG, test_results
    CONFIG = config
    
    # CRITICAL: Clear the global list to ensure a clean run for this file
    test_results = []

    # Ensure the test class is in a clean state before starting
    TestStoredProcedure.conn = None
    TestStoredProcedure.cursor = None

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestStoredProcedure)

    if suite.countTestCases() == 0:
        log_error("Failed to load any tests from TestStoredProcedure.")
        # Return a failure message in the expected format
        return [("Unknown", "Test Loading", "❌ Failed", "Could not load any tests.", "")]

    # Inject the target SQL file into every test case instance in the suite
    for test in suite:
        test.sql_file = sql_file_path

    # Use a dummy stream for the runner; our real results are collected in `run_test_with_capture`
    runner = unittest.TextTestRunner(stream=io.StringIO(), verbosity=2)

    try:
        # Manually manage setup/teardown for robust connection handling
        TestStoredProcedure.setUpClass()
        runner.run(suite)
    finally:
        # Crucially, always ensure the connection is closed
        TestStoredProcedure.tearDownClass()

    # The global `test_results` list has been populated by the tests. Return it.
    return test_results







class TestStoredProcedure(unittest.TestCase):
    conn = None
    input_file_path = None 



    # The constructor should remain standard for unittest compatibility
    # We will set sql_file after creating the instance.
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)

    # def __init__(self,*args, **kwargs):
    #     super().__init__(*args, **kwargs)
        # self.proc_name = proc_name
 
        # self.sql_file = sql_file
        # filename = os.path.basename(self.sql_file)  
        # m = re.match(r'.*_(?P<proc>[^.]+)\.sql$', filename)
        # self.proc_name = m.group('proc') 

        # PYUNIT_OUTPUT_TABLE = "TEST_RESULTS_LOG"
        # METADATA_TABLE = "PROCEDURES_METADATA"
        # self.PYUNIT_OUTPUT_TABLE = PYUNIT_OUTPUT_TABLE
        # self.METADATA_TABLE = METADATA_TABLE

    @classmethod
    def setUpClass(cls):
        """Setup Snowflake connection before tests."""
        if not CONFIG or "SNOWFLAKE_CONFIG" not in CONFIG:
            # This will cause tests to fail with a clear message if config isn't set
            raise ValueError("Snowflake configuration not provided to the test module.")
        
        # Establish the connection ONCE and store it on the class
        if cls.conn is None or cls.conn.is_closed():
            try:
                cls.conn = snowflake.connector.connect(**CONFIG["SNOWFLAKE_CONFIG"])
                cls.cursor = cls.conn.cursor()
                log_info("Snowflake connection established for testing.")
            except Exception as e:
                # This makes debugging connection errors much easier
                raise ConnectionError(f"Failed to connect to Snowflake for testing: {e}") from e
        # cls.conn = snowflake.connector.connect(**self.snowflake_config)
        # cls.cursor = cls.conn.cursor()

            # Ensure TEST_RESULTS_LOG table exists
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS TEST_RESULTS_LOG (
            TEST_CASE_ID     STRING,
            TEST_CASE_NAME   STRING,
            PROCEDURE_NAME   STRING,
            TEST_TIMESTAMP   TIMESTAMP_NTZ,
            STATUS           STRING,
            ERRORS           STRING
        );
        """
        try:
            cls.cursor.execute(create_sql)
            cls.conn.commit()
            log_info("Ensured TEST_RESULTS_LOG table exists.")
        except Exception as e:
            log_error(f"Error creating TEST_RESULTS_LOG table: {e}")
            raise

    @classmethod
    def tearDownClass(cls):
        """Close the connection after tests."""
        if cls.cursor:
            cls.cursor.close()
        if cls.conn and not cls.conn.is_closed():
            cls.conn.close()
            log_info("Snowflake connection closed.")
        # Reset class state for any subsequent runs from the UI
        cls.conn = None # <-- CRITICAL RESET
        cls.cursor = None
        generate_html_report(results=test_results)


    def setUp(self):
        
        """This method runs before EACH test method."""
        self.assertIsNotNone(self.sql_file, "sql_file was not set on the test instance.")
        
        filename = os.path.basename(self.sql_file)
        
        # --- ROBUST REGEX LOGIC ---
        # Try the pattern with an underscore first
        match = re.match(r'.*_(?P<proc>[^.]+)\.sql$', filename)
        
        # If that fails, try a simpler pattern (the whole filename without extension)
        if not match:
            match = re.match(r'(?P<proc>[^.]+)\.sql$', filename)
            
        if not match:
            # If both patterns fail, we must stop the test with a clear error.
            self.fail(
                f"FATAL: Could not extract a procedure name from the filename '{filename}'. "
                "Ensure it follows a recognized pattern (e.g., 'v1_myproc.sql' or 'myproc.sql')."
            )
        # ---------------------------
            
        # If we get here, a match was found.
        self.proc_name = match.group('proc')
        
        # Log this for debugging purposes
        log_info(f"Setting up test for procedure: '{self.proc_name}' from file: '{filename}'")
        
        self.PYUNIT_OUTPUT_TABLE = "TEST_RESULTS_LOG"
        self.METADATA_TABLE = "PROCEDURES_METADATA"



    def run_test_with_capture(self, test_func, test_name="test_function"):
        global test_case_id_counter

                # ─── 0) EARLY SKIP CHECK ──────────────────────────────────────────────────────
        # Check if TEST_RESULTS_LOG table exists
        try:
            # Attempt a simple SELECT. If table does not exist, an exception will be thrown.
            check_sql = f"""
                SELECT STATUS
                  FROM {self.PYUNIT_OUTPUT_TABLE}
                 WHERE TEST_CASE_NAME = %s
                   AND PROCEDURE_NAME = %s
            """
            self.cursor.execute(check_sql, (test_name, self.proc_name))
            row = self.cursor.fetchone()
            if row and row[0] == "✅ Success" and test_name != "test_create_procedure_from_file":
                log_info(f"Skipping `{test_name}` for `{self.proc_name}` – already succeeded.")
                return  # Do not re‐run a test that has previously passed
        except Exception as e:
            # Likely the table doesn’t exist yet (but setUpClass should have created it).
            # If it's some other error, we log and proceed to run the test anyway.
            log_info(f"No prior success check – will run test. (Details: {e})")


        # --- 2) Increment counter and create ID ---
        test_case_id_counter += 1
        test_case_id = str(test_case_id_counter)

        """Runs a test function and captures its output."""
        output_capture = io.StringIO()
        sys.stdout = output_capture
        sys.stderr = output_capture  # Capture errors as well

        try:
            test_func()
            status = "✅ Success"
            reason = "-"
        except Exception as e:
            status = "❌ Failed"
            reason = str(e)

        sys.stdout = sys.__stdout__  # Restore standard output
        sys.stderr = sys.__stderr__  # Restore error output

        test_results.append((self.proc_name, test_name, status, reason, output_capture.getvalue()))

        # 2) Immediately INSERT into Snowflake Pyunit test results table
        # → get a timezone‐aware UTC datetime, then format it
        utc = datetime.now(timezone.utc)
        # if you still want a naive string (e.g. for TIMESTAMP_NTZ), drop the tzinfo:
        naive = utc.replace(tzinfo=None)
        # format to millisecond precision
        ts = naive.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        # ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        merge_sql = f"""
        MERGE INTO {self.PYUNIT_OUTPUT_TABLE} AS T
        USING (
          SELECT
           %s AS TEST_CASE_ID,
            %s AS TEST_CASE_NAME,
            %s AS PROCEDURE_NAME,
            TO_TIMESTAMP_NTZ(%s) AS TEST_TIMESTAMP,
            %s AS STATUS,
            %s AS ERRORS
        ) AS S
        ON
            T.TEST_CASE_NAME = S.TEST_CASE_NAME
            AND T.PROCEDURE_NAME = S.PROCEDURE_NAME
        WHEN MATCHED THEN 
            UPDATE SET
              TEST_TIMESTAMP = S.TEST_TIMESTAMP,
              STATUS         = S.STATUS,
              ERRORS         = S.ERRORS,
              TEST_CASE_ID   = S.TEST_CASE_ID
        WHEN NOT MATCHED THEN
            INSERT (
              TEST_CASE_ID,
              TEST_CASE_NAME,
              PROCEDURE_NAME,
              TEST_TIMESTAMP,
              STATUS,
              ERRORS
            )
            VALUES (
              S.TEST_CASE_ID,
              S.TEST_CASE_NAME,
              S.PROCEDURE_NAME,
              S.TEST_TIMESTAMP,
              S.STATUS,
              S.ERRORS
            )
        ;
        """


        # insert_sql = f"""
        #   INSERT INTO {self.PYUNIT_OUTPUT_TABLE} (
        #     TEST_CASE_ID,
        #     TEST_CASE_NAME,
        #     PROCEDURE_NAME,
        #     TEST_TIMESTAMP,
        #     STATUS,
        #     ERRORS
        #   ) VALUES (%s, %s, %s, TO_TIMESTAMP_NTZ(%s), %s, %s)
        # """
        # You can generate a unique TEST_CASE_ID however you like; here we just combine proc+test name+ts
        # test_case_id = f"{self.proc_name}::{test_name}::{ts}"
        try:
            self.cursor.execute(
                merge_sql,
                (test_case_id, test_name, self.proc_name, ts, status, reason)
            )
            self.conn.commit()
        except Exception as sf_e:
            log_error(f"Failed to log to Snowflake for {self.proc_name}/{test_name}: {sf_e}")

        
        # ── 5) If this test was a SUCCESS, mark the proc deployed in the metadata table
        if status == "✅ Success" and test_name == "test_procedure_execution":
            clean_proc_name = re.sub(r'\(.*\)$', '', self.proc_name)
            try:
                update_sql = f"""
                UPDATE {self.METADATA_TABLE}
                   SET IS_DEPLOYED = TRUE
                 WHERE PROCEDURE_NAME = %s
                """
                self.cursor.execute(update_sql, (clean_proc_name))
                self.conn.commit()
                log_info(f"Marked {self.proc_name} as deployed in {self.METADATA_TABLE}")
            except Exception as upd_e:
                log_error(f"Failed to update IS_DEPLOYED for {clean_proc_name}: {upd_e}")


    def test_create_procedure_from_file(self):
        def test_logic():
            with open(self.sql_file, "r") as file:
                sql_script = file.read()
            self.cursor.execute(sql_script)
            log_info(f"Stored procedure {self.proc_name} executed successfully.")

        self.run_test_with_capture(test_logic, "test_create_procedure")



    def test_procedure_execution(self):
        """Test whether the stored procedure runs successfully."""

        # 2) Fetch its PARAMETERS definition from Snowflake metadata:
        self.cursor.execute(
            "SELECT PARAMETERS FROM PROCEDURES_METADATA WHERE PROCEDURE_NAME = %s",
            (self.proc_name,)
        )
        row = self.cursor.fetchone()
        params_str = row[0] if row and row[0] else ""
        
        # 3) Count declared parameters (commas → count+1), or zero if empty
        num_params = len(params_str.split(',')) if params_str.strip() else 0

        # 4) Build the "(NULL, NULL, ...)" suffix
        nulls = ", ".join("NULL" for _ in range(num_params))
        full_proc_call = f"{self.proc_name}({nulls})"

        def test_logic():
            self.cursor.execute(f"CALL {full_proc_call}")
            result = self.cursor.fetchall()
            self.assertIsNotNone(result, f"Stored procedure {full_proc_call} returned None")
            log_info(f"Stored procedure {full_proc_call} executed and returned results.")

        self.run_test_with_capture(test_logic, "test_procedure_execution")

    

