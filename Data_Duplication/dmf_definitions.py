# dmf_definitions.py

def get_dmf_functions():
    """
    Returns a dictionary of Snowflake Data Metric Functions.
    - Key: User-friendly name for the function.
    - Value: The SQL function string. Use {column} as a placeholder for the column name.
    """
    dmf_functions = {
        # --- Row Level Metrics ---
        "ROW_COUNT": "COUNT(*)",

        # --- Column Level Metrics ---
        "NULL_COUNT": "COUNT_IF({column} IS NULL)",
        "NOT_NULL_COUNT": "COUNT_IF({column} IS NOT NULL)",
        "UNIQUE_COUNT": "COUNT(DISTINCT {column})",
        "DUPLICATE_COUNT": "COUNT({column}) - COUNT(DISTINCT {column})",

        # --- Numeric Metrics ---
        "AVERAGE": "AVG({column})",
        "SUM": "SUM({column})",
        "MIN": "MIN({column})",
        "MAX": "MAX({column})",
        "STDDEV": "STDDEV({column})",

        # --- Text Metrics ---
        "MIN_LENGTH": "MIN(LENGTH({column}))",
        "MAX_LENGTH": "MAX(LENGTH({column}))",
        "AVG_LENGTH": "AVG(LENGTH({column}))",
    }
    return dmf_functions