# extract_schema_by_schema.py
import snowflake.connector
from config import SNOWFLAKE_CONFIG  # make sure this dict contains a 'database' key and a 'schema' key

def get_table_schema(cursor, database_name, schema_name):
    # database_name must come from a trusted config (we interpolate it directly).
    # schema_name is passed as a parameter to avoid SQL injection.
    query = f'''
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
    FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = %s
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
    '''
    cursor.execute(query, (schema_name,))
    return cursor.fetchall()

def preprocess_schema(schema_details):
    processed_schema = []
    table_columns = {}

    for row in schema_details:
        schema, table, column, data_type, is_nullable, max_length = row

        null_status = "NOT NULL" if is_nullable == "NO" else "NULL"
        length_info = f'({max_length})' if max_length is not None else ''
        column_entry = f'{column} ({data_type.lower()}{length_info}, {null_status})'

        table_key = f'{schema}.{table}'
        table_columns.setdefault(table_key, []).append(f'- {column_entry}')

    for table, columns in table_columns.items():
        processed_schema.append(f'Table: {table}')
        processed_schema.append('Columns:')
        processed_schema.extend(columns)
        processed_schema.append('')

    return processed_schema

def save_schema_to_file(schema_details, output_file):
    processed_schema = preprocess_schema(schema_details)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(processed_schema))

def main():
    output_file = "Data_Duplication/input_and_output_files/original_schema.txt"

    # Expect SNOWFLAKE_CONFIG to include at least 'user','password','account','database' and optionally 'schema'
    target_schema = SNOWFLAKE_CONFIG.get('schema')  # <-- the schema you want to extract
    if not target_schema:
        raise ValueError("Please set 'schema' in SNOWFLAKE_CONFIG in your config.py (the schema to extract).")

    ctx = None
    cursor = None
    try:
        ctx = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = ctx.cursor()
        db_name = SNOWFLAKE_CONFIG.get('database') or ctx.database  # prefer config value
        if not db_name:
            # fallback to current DB for the session
            cursor.execute("SELECT CURRENT_DATABASE()")
            db_name = cursor.fetchone()[0]

        schema_details = get_table_schema(cursor, db_name, target_schema)
        save_schema_to_file(schema_details, output_file)
        print(f"Schema details for schema '{target_schema}' (database: '{db_name}') saved to {output_file}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if ctx is not None:
            try:
                ctx.close()
            except Exception:
                pass

if __name__ == '__main__':
    main()
