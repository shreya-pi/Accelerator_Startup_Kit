# --- START OF FILE py_output.py ---
import streamlit as st
import snowflake.connector
import sys

# It's better to import config here rather than assuming it's globally available
# This makes the class more self-contained.
# try:
#     from config import SNOWFLAKE_CONFIG
# except ImportError:
#     print("ERROR: config.py not found. Please ensure it is in the same directory.", file=sys.stderr)
#     sys.exit(1)

class PyOutput:
    def __init__(self, config:dict):
        if not config or "SNOWFLAKE_CONFIG" not in config:
            st.error("Configuration is missing or invalid.")
            st.stop()
        
        # Store the needed config parts as instance variables
        self.snowflake_config = config["SNOWFLAKE_CONFIG"]
        # self.sql_server_config = config["SQL_SERVER_CONFIG"]

        # It's good practice to get the table name from the config file if possible,
        # but hardcoding is fine for this example.
        self.PYUNIT_OUTPUT_TABLE = "TEST_RESULTS_LOG"

    def display_PyOutput(self):
        """
        Connects to Snowflake and fetches all records from the test log table.

        Returns:
            tuple: A tuple containing:
                   - list: A list of tuples, where each tuple is a row.
                   - list: A list of strings representing the column headers.
        """
        try:
            conn = snowflake.connector.connect(
                user=self.snowflake_config['user'],
                password=self.snowflake_config['password'],
                account=self.snowflake_config['account'],
                warehouse=self.snowflake_config['warehouse'],
                database=self.snowflake_config['database'],
                schema=self.snowflake_config['schema']
            )
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}", file=sys.stderr)
            # Return empty values on connection failure
            return [], []

        cursor = conn.cursor()
        try:
            # Execute the query
            cursor.execute(f"SELECT * FROM {self.PYUNIT_OUTPUT_TABLE} ORDER BY TEST_TIMESTAMP DESC")

            # Fetch all results
            results = cursor.fetchall()

            # Get column headers from the cursor description
            column_names = [desc[0] for desc in cursor.description]

        except Exception as e:
            print(f"Error querying table {self.PYUNIT_OUTPUT_TABLE}: {e}", file=sys.stderr)
            return [], []
        finally:
            # Always ensure the connection is closed
            cursor.close()
            conn.close()

        return results, column_names

if __name__ == "__main__":
    py_output = PyOutput()
    data, headers = py_output.display_PyOutput()
    if data:
        print("--- Test Results ---")
        print(headers)
        for row in data:
            print(row)