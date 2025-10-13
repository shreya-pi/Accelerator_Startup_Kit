
### File-by-File Explanation

**`helper_scripts/`**: This directory promotes clean code by separating concerns.
    *   **`col_desc.py`**: Contains the `SnowflakeSchemaDescriber` class. It uses a sentence-transformer model to find the relevant schema from `original_schema.txt` and calls Cortex to generate descriptions.
    *   **`cortex_complete.py`**: Contains a focused function that takes a prompt and model name, and executes the `SNOWFLAKE.CORTEX.COMPLETE` SQL command, handling options like `max_tokens`.
    *   **`dmf_definitions.py`**: Defines a simple Python dictionary mapping user-friendly function names (e.g., "Find Null Count in Column") to their corresponding SQL code templates (e.g., `SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)`). This makes the main app code cleaner and the functions easier to manage.
    *   **`log.py`**: A standard logging setup module. It would configure a logger to write timestamped messages to a file in the `logs/` directory for easier debugging.
    *   **`process_output.py`**: Could contain functions to format raw data from Snowflake into more presentable formats, such as converting a DataFrame to a styled Markdown table or a JSON object.
