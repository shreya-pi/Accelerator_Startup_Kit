#!/usr/bin/env python3
"""
json_to_snowflake.py

Parses a JSON file from a Snowflake internal stage, normalizes it into a
relational schema, and creates/loads corresponding tables in Snowflake.

Main entry point:
  process_json_from_stage_to_snowflake(conn_params, database, schema, stage_name, file_path)
"""

import json
import os
import re
import tempfile
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Union
# from config import SNOWFLAKE_CONFIG
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pathlib
# -----------------------
# Helpers (mostly unchanged)
# -----------------------
def sanitize_name(name: str) -> str:
    """Make a safe table/column name: uppercase, alnum + underscore"""
    name = re.sub(r'[^0-9a-zA-Z_]', '_', name)
    name = re.sub(r'__+', '_', name)
    name = name.strip('_').upper() # Use uppercase for Snowflake convention
    if not name:
        return 'COL'
    if name[0].isdigit():
        name = '_' + name
    return name

def load_json_from_local_file(path: str) -> List[Any]:
    """
    Load JSON from a local file path.
    Handles both standard JSON arrays and NDJSON (newline-delimited).
    Returns: list of top-level records
    """
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read().strip()
        if not text:
            return []
        # Try NDJSON/line-by-line first
        lines = text.splitlines()
        if len(lines) > 1:
            try:
                records = [json.loads(line) for line in lines if line.strip()]
                return records
            except json.JSONDecodeError:
                # If NDJSON fails, fall back to parsing the whole file
                pass
        # Parse as a single JSON array or object
        obj = json.loads(text)
        return obj if isinstance(obj, list) else [obj]

# -----------------------
# Core normalizer (unchanged, but column names are now uppercased by sanitize_name)
# -----------------------
class Normalizer:
    def __init__(self, root_name: str = "ROOT"):
        self.root_name = sanitize_name(root_name)
        self.tables: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.id_counters: Dict[str, int] = defaultdict(int)

    def _next_id(self, table: str) -> int:
        self.id_counters[table] += 1
        return self.id_counters[table]

    def _is_primitive(self, v: Any) -> bool:
        return v is None or isinstance(v, (str, int, float, bool))

    def _record_primitives(self, table: str, row: Dict[str, Any]):
        self.tables[table].append(row)

    def process(self, records: List[Any]):
        for rec in records:
            root_table = self.root_name
            root_id = self._next_id(root_table)
            row = {'ID': root_id} # Use uppercase for Snowflake convention
            if self._is_primitive(rec):
                row['VALUE'] = rec
                self._record_primitives(root_table, row)
            elif isinstance(rec, dict):
                for k, v in rec.items():
                    col = sanitize_name(k)
                    if self._is_primitive(v):
                        row[col] = v
                    elif isinstance(v, dict):
                        child_table = f"{root_table}_{sanitize_name(k)}"
                        child_id = self._next_id(child_table)
                        child_row = {'ID': child_id, f"{root_table}_ID": root_id}
                        for ck, cv in v.items():
                            if self._is_primitive(cv):
                                child_row[sanitize_name(ck)] = cv
                        self.tables[child_table].append(child_row)
                        self._recurse_object(v, child_table, child_row['ID'], parent_table=root_table)
                    elif isinstance(v, list):
                        arr_table = f"{root_table}_{sanitize_name(k)}"
                        self._process_array(v, arr_table, root_table_id=root_id, parent_table=root_table)
                    else:
                        row[col] = str(v)
                self._record_primitives(root_table, row)
            elif isinstance(rec, list):
                # This case is for a top-level array of arrays, less common
                arr_table = f"{root_table}_ARRAY"
                self._process_array(rec, arr_table, root_table_id=root_id, parent_table=root_table)
                self._record_primitives(root_table, row)
            else:
                row['VALUE'] = str(rec)
                self._record_primitives(root_table, row)

    def _recurse_object(self, obj: dict, table: str, parent_id: int, parent_table: str):
        for k, v in obj.items():
            if self._is_primitive(v):
                continue
            col_name = sanitize_name(k)
            if isinstance(v, dict):
                child_table = f"{table}_{col_name}"
                child_id = self._next_id(child_table)
                child_row = {'ID': child_id, f"{table}_ID": parent_id}
                for ck, cv in v.items():
                    if self._is_primitive(cv):
                        child_row[sanitize_name(ck)] = cv
                self.tables[child_table].append(child_row)
                self._recurse_object(v, child_table, child_row['ID'], parent_table=table)
            elif isinstance(v, list):
                arr_table = f"{table}_{col_name}"
                self._process_array(v, arr_table, root_table_id=parent_id, parent_table=table)

    def _process_array(self, arr: list, arr_table: str, root_table_id: int, parent_table: str):
        arr_table = sanitize_name(arr_table)
        for elem in arr:
            row_id = self._next_id(arr_table)
            row = {'ID': row_id, f"{parent_table}_ID": root_table_id}
            if self._is_primitive(elem):
                row['VALUE'] = elem
                self.tables[arr_table].append(row)
            elif isinstance(elem, dict):
                for k, v in elem.items():
                    if self._is_primitive(v):
                        row[sanitize_name(k)] = v
                self.tables[arr_table].append(row)
                self._recurse_object(elem, arr_table, parent_id=row_id, parent_table=parent_table)
            elif isinstance(elem, list):
                # Handle nested arrays
                child_table = f"{arr_table}_ITEM"
                self._process_array(elem, child_table, root_table_id=row_id, parent_table=arr_table)
            else:
                row['VALUE'] = str(elem)
                self.tables[arr_table].append(row)

# -----------------------
# Type inference & DDL (unchanged, types are compatible with Snowflake)
# -----------------------
def infer_column_type(values: List[Any]) -> str:
    vals = [v for v in values if v is not None]
    if not vals: return 'VARCHAR'
    if all(isinstance(v, bool) for v in vals): return 'BOOLEAN'
    if all(isinstance(v, int) and not isinstance(v, bool) for v in vals): return 'NUMBER'
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
        return 'FLOAT' if any(isinstance(v, float) for v in vals) else 'NUMBER'
    if all(isinstance(v, str) and re.match(r'^\d{4}-\d{2}-\d{2}', v) for v in vals):
        return 'TIMESTAMP_NTZ'
    return 'VARCHAR'

def generate_ddl(table: str, rows: List[Dict[str, Any]]) -> str:
    all_cols = set().union(*(d.keys() for d in rows))
    cols_order = []
    if 'ID' in all_cols:
        cols_order.append('ID')
        all_cols.remove('ID')
    fks = sorted([c for c in all_cols if c.endswith('_ID')])
    cols_order.extend(fks)
    cols_order.extend(sorted(all_cols - set(fks)))

    col_lines = []
    for c in cols_order:
        vals = [r.get(c) for r in rows]
        col_type = infer_column_type(vals)
        col_lines.append(f'  "{c}" {col_type}')

    ddl = f'CREATE OR REPLACE TABLE "{table}" (\n' + ",\n".join(col_lines) + "\n);"
    return ddl

def list_files_in_stage(stage_name: str, config:dict) -> List[str]:
    """
    Executes a LIST command and returns a list of file paths in the stage.
    """
    print(f"Listing files in stage '{stage_name}'...")
    SNOWFLAKE_CONFIG = config.get('SNOWFLAKE_CONFIG')
    cur = None
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()
        cur.execute(f'USE DATABASE "{SNOWFLAKE_CONFIG["database"]}"')
        cur.execute(f'USE SCHEMA "{SNOWFLAKE_CONFIG["schema"]}"')
        cur.execute(f"LIST {stage_name}")
        
        # The file path is the first column of the result set
        files = [row[0] for row in cur.fetchall()]
        
        # Filter for common JSON extensions for cleaner lists
        json_files = [f for f in files if f.lower().endswith(('.json', '.jsonl', '.ndjson'))]
        print(f"Found {len(json_files)} JSON file(s).")
        return json_files
    finally:
        if cur:
            cur.close()


def process_json_from_stage_to_snowflake(
    stage_name: str,
    file_path_in_stage: str, config:dict
) -> Dict[str, Dict[str, Any]]:
    """
    Downloads a JSON file from a stage, normalizes it, and loads it into Snowflake tables.

    Args:
        conn_params: Dictionary of connection parameters for Snowflake.
        database: The target database.
        schema: The target schema.
        stage_name: The internal stage name (e.g., '@MY_STAGE').
        file_path_in_stage: The relative path to the JSON file in the stage.

    Returns:
        A dictionary with results for each table created.
    """
    results = {}
    with tempfile.TemporaryDirectory() as temp_dir:
        # --- FIX STARTS HERE ---
        # Resolve the temporary directory path to its full, absolute form.
        # This expands '~' and removes any relative pathing (e.g., '..').
        # This is the crucial step to prevent the Snowflake error.
        # absolute_temp_dir = os.path.abspath(os.path.expanduser(temp_dir))
        # --- FIX ENDS HERE ---

        conn = None
        try:
            SNOWFLAKE_CONFIG = config.get('SNOWFLAKE_CONFIG')
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cur = conn.cursor()
            print("Successfully connected to Snowflake.")

            cur.execute(f'USE DATABASE "{SNOWFLAKE_CONFIG["database"]}"')
            cur.execute(f'USE SCHEMA "{SNOWFLAKE_CONFIG["schema"]}"')
            print(f"Using database '{SNOWFLAKE_CONFIG["database"]}' and schema '{SNOWFLAKE_CONFIG["schema"]}'.")


            temp_path_obj = pathlib.Path(temp_dir).resolve()
            
            # 2. Convert to a POSIX-style path for the GET command.
            download_dir_for_snowflake = temp_path_obj.as_posix()
            
            local_file_path = temp_path_obj / os.path.basename(file_path_in_stage)

            # 3. Construct the GET command
            get_sql = f"GET {stage_name}/{file_path_in_stage} file://{download_dir_for_snowflake}"

            # # 4. !! CRITICAL DEBUG STEP !! Print the exact command before executing it.
            # print("\n" + "="*20 + " DEBUGGING " + "="*20)
            # print(f"Temporary directory created: {temp_dir}")
            # print(f"Resolved path for Snowflake: {download_dir_for_snowflake}")
            # print(f"Executing SQL command: {get_sql}")
            # print("="*53 + "\n")

            print(f"Downloading '{os.path.basename(file_path_in_stage)}' from stage '{stage_name}'...")
            cur.execute(get_sql) 

            # print(f"Downloaded to temporary file: {local_file_path}")


            # 2. Load and normalize the JSON data
            records = load_json_from_local_file(local_file_path)
            if not records:
                print("JSON file is empty. Nothing to process.")
                return {}

            root_name = sanitize_name(os.path.splitext(os.path.basename(file_path_in_stage))[0])
            norm = Normalizer(root_name=root_name)
            norm.process(records)
            print(f"Normalized JSON into {len(norm.tables)} tables.")

            # 3. Create tables and load data into Snowflake
            for table_name, rows in norm.tables.items():
                print(f"\nProcessing table: {table_name}")
                if not rows:
                    print(f" -> No data for table '{table_name}'. Skipping.")
                    continue
                
                ddl = generate_ddl(table_name, rows)
                print(f" -> Executing DDL:\n{ddl}")
                cur.execute(ddl)
                
                df = pd.DataFrame(rows)
                ddl_cols = [c.strip().split()[0].strip('"') for c in ddl.splitlines()[1:-1]]
                df = df[ddl_cols]
                
                print(f" -> Loading {len(df)} rows into '{table_name}'...")
                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    df,
                    table_name,
                    auto_create_table=False,
                    overwrite=True,
                )
                if success:
                    print(f" -> Successfully loaded {nrows} rows.")
                    results[table_name] = {"rows_loaded": nrows, "columns": len(df.columns)}
                else:
                    print(f" -> FAILED to load data into {table_name}.")
                    results[table_name] = {"rows_loaded": 0, "error": "write_pandas failed"}

        except snowflake.connector.Error as e:
            print(f"Snowflake Error: {e}")
            raise
        finally:
            if 'cur' in locals() and cur: cur.close()
            if conn: conn.close()
            print("Snowflake connection closed.")

    return results

# # -----------------------
# # Example Usage
# # -----------------------
# if __name__ == "__main__":

#     SOURCE_STAGE = "@JSON_STAGE"
#     # Path to the file *within* the stage
#     JSON_FILE_IN_STAGE = "inventory.json"

#     print("Starting JSON to Snowflake process...")


    
#     # --- Step 1: Connect and get the list of files ---
#     selected_file = None
#     conn = None
#     try:
#         print("Connecting to Snowflake to retrieve file list...")
#         conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        
#         files_in_stage = list_files_in_stage( SOURCE_STAGE)

#         if not files_in_stage:
#             raise SystemExit(f"No JSON files found in stage '{SOURCE_STAGE}'. Exiting.")

#         # --- Step 2: Prompt user for selection ---
#         print("\nPlease select a file to process:")
#         for i, file_path in enumerate(files_in_stage):
#             # Just show the filename, not the full stage path
#             file_name = os.path.basename(file_path)
#             print(f"  [{i + 1}] {file_name}")

#         while True:
#             try:
#                 choice_str = input(f"\nEnter number (1-{len(files_in_stage)}): ")
#                 choice_idx = int(choice_str) - 1
#                 if 0 <= choice_idx < len(files_in_stage):
#                     # The full path from the stage is what we need
#                     selected_file = os.path.basename(files_in_stage[choice_idx])
#                     break
#                 else:
#                     print("Invalid number. Please try again.")
#             except ValueError:
#                 print("Invalid input. Please enter a number.")
                
#     except snowflake.connector.Error as e:
#         print(f"A Snowflake error occurred: {e}")
#         raise
#     finally:
#         if conn:
#             conn.close()
#             print("Initial Snowflake connection closed.")

#     # --- Step 3: If a file was selected, process it ---
#     if selected_file:
#         print(f"\nProceeding to process selected file: '{selected_file}'\n")
#         try:
#             final_results = process_json_from_stage_to_snowflake(
#                 stage_name=SOURCE_STAGE,
#                 file_path_in_stage=selected_file,
#             )
#             print("\n--- Process Summary ---")
#             if final_results:
#                 for table, info in final_results.items():
#                     print(f" - Table '{table}': Loaded {info['rows_loaded']} rows and {info['columns']} columns.")
#             else:
#                 print("No tables were created or loaded.")
#             print("-----------------------")
#         except Exception as e:
#             print(f"\nAn error occurred during processing: {e}")




#     # Execute the main function
#     try:
#         final_results = process_json_from_stage_to_snowflake(
#             stage_name=SOURCE_STAGE,
#             file_path_in_stage=JSON_FILE_IN_STAGE,
#         )

#         print("\n--- Process Summary ---")
#         if final_results:
#             for table, info in final_results.items():
#                 print(f" - Table '{table}': Loaded {info['rows_loaded']} rows and {info['columns']} columns.")
#         else:
#             print("No tables were created or loaded.")
#         print("-----------------------")

#     except Exception as e:
#         print(f"\nAn error occurred during the process: {e}")