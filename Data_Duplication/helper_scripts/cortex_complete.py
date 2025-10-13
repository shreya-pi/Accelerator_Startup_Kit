import os
import json
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from config import SNOWFLAKE_CONFIG
from typing import Dict, Any, Union

def get_cortex_completion(prompt: str, model: str = 'claude-3-5-sonnet') -> Union[Dict[str, Any], str]:
    """
    Connects to Snowflake and executes a prompt on Snowflake Cortex.

    Args:
        prompt (str): The prompt to send to the language model.
        model (str): The name of the Cortex model to use. 
                     Defaults to 'mistral-large'. Other options include
                     'llama2-70b-chat', etc.

    Returns:
        str: The response from the language model.
    """
    # conn = None
    try:
        # --- Connection ---
        print("Connecting to Snowflake...")
        sf_cfg = SNOWFLAKE_CONFIG
        conn = snowflake.connector.connect(
            user=sf_cfg['user'],
            password=sf_cfg['password'],
            account=sf_cfg['account'],
            warehouse=sf_cfg['warehouse'],
            database=sf_cfg['database'],
            schema=sf_cfg['schema']
        )
        print("Connection successful.")

        # --- SQL Execution ---
        # Using parameter binding (%s) to prevent SQL injection

        # sql_command = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)"

        sql_command = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, [{'role':'user', 'content':%s}], {'max_tokens': %s})"

        # sql_command= '''SELECT AI_COMPLETE(model => %s, prompt => %s,  model_parameters  => OBJECT_CONSTRUCT(
        #                  'max_tokens', %s
        #                ), show_details => TRUE)'''


        max_tokens = 8192 # Adjust as needed
        params = (model, prompt, max_tokens)



        print(f"\nExecuting prompt on Cortex with model '{model}'...")
        cursor = conn.cursor()





        
        cursor.execute(sql_command, params)
        # cursor.execute(sql_command)

        
        # --- Fetching Result ---
        # The result is a single row with a single column
        result_row = cursor.fetchone()

        if result_row and result_row[0]:
            # 2. The result is a tuple, get the first element which is the JSON string.
            response_json_string = result_row[0]
            
            # 3. Parse the JSON string into a Python dictionary.
            parsed_json = json.loads(response_json_string)
            
            return parsed_json
        else:
            return "Error: No response received from Snowflake Cortex."
        # json_data = [json.loads(row[0]) for row in result]
        
        # if result:
        #     return result
        # else:
        #     return "Error: No response received from Snowflake Cortex."

    except ProgrammingError as e:
        print(f"Snowflake Programming Error: {e}")
        return f"Error executing query. Does the role have USAGE on the SNOWFLAKE.CORTEX functions? Details: {e}"
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return f"An error occurred: {e}"
    finally:
        # --- Cleanup ---
        if conn:
            cursor.close()
            conn.close()
            print("\nConnection closed.")

# --- Main execution block ---
if __name__ == "__main__":
    # output_filename = "Data_Duplication/input_and_output_files/schema.json"
    output_filename = "Data_Duplication/input_and_output_files/views.json"


    # with open('Data_Duplication/input_and_output_files/original_schema.txt', 'r', encoding='utf-8') as file:
    #     schema_content = file.read()
    with open('Data_Duplication/input_and_output_files/formatted_schema.md', 'r', encoding='utf-8') as file:
        schema_content = file.read()
    

    # my_prompt = "Explain the importance of the OSI model in computer networking in three key points."
    my_prompt = f''' ########################
    Using the provided schema:
    
    Analyze and reconstruct the relational data model, identifying all primary keys (PKs), foreign keys (FKs), cardinality, and relational constraints.
    
    While doing so, do not treat any column named ID as a primary key, as these are falsely generated and do not indicate actual uniqueness.
    
    Instead, infer primary keys based on column semantics, naming conventions, or relationships across tables.
    
    Reprint the schema, listing all columns for each table, and clearly mark, if applicable, on the right side of each column:
    Primary Keys (Primary_Key), 
    In case of multiple primary keys, state it as Composite Primary Keys (Composite_Primary_Key) and list all applicable columns, 
    Foreign Keys (Foreign_Key) along with referenced table and column
    
    Ensure that foreign key mappings reflect the relational structure implied by the schema.
    If no primary and/or foreign keys can be inferred, in such cases, set the PK and/or FK as null
    
    Use a clean and readable format. Mention all the columns in each table while generating the response. Do not omit any columns for the sake of brevity.
    ########################
    schema:– {schema_content}''' 


    my_prompt_1 = f'''Based on the given schema and its defined relational structure, generate two Snowflake SQL views per table as follows. Ensure both views are generated for **EVERY** table in the schema.

🔹 View 1: <table_name>_duplicate_records
	• Use a Common Table Expression (CTE) and the ROW_NUMBER() OVER (PARTITION BY ...) window function to detect duplicates.
	• Partition the data using all columns except primary keys, IDs, or timestamp fields, since those might falsely appear unique.
	• Identify duplicate rows as those where ROW_NUMBER() > 1.

🔹 View 2: <table_name>_clean_view
	• Create a deduplicated version of the table using a CTE and the ROW_NUMBER() OVER (PARTITION BY ...) logic.
	• Retain only rows where ROW_NUMBER() = 1, effectively removing duplicates.
	• Ensure that this cleaned data maintains referential integrity by applying INNER JOINs with related tables using foreign key constraints (as defined in the schema).
	• All joins must reflect the relationships described in the schema (e.g., <table_a>.foreign_key = <table_b>.primary_key).

Additional Instructions:
	• Use descriptive aliases and CTE names for clarity.
	• Avoid relying on surrogate keys like IDs or timestamps for identifying logical duplicates.
	• Assume Snowflake or ANSI-compliant SQL unless otherwise specified.
	• Name the views following this convention:
		○ table_name_duplicate_records
          table_name_clean_view
    • Ensure both views are generated for **EVERY** table in the schema.
    • Ensure views are generated for EVERY table in the schema. Do not skip any tables for the sake of brevity.

    Schema:
{schema_content} '''
    


    print("--- Snowflake Cortex Completion Example ---")
    
    # Call the function and get the response
    response = get_cortex_completion(my_prompt_1, model='snowflake-llama-3.3-70b')

    # Print the formatted response
    # print("\n" + "="*50)
    # print("PROMPT:")
    # print(my_prompt)
    # print("="*50)

    # print("CORTEX RESPONSE:")
    # print(response)
    # print("="*50)


    # --- Write the response to a text file ---
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(response, f,ensure_ascii=False, indent=4)


        print(f"\n✅ Response successfully saved to '{output_filename}'")
    except IOError as e:
        print(f"\n❌ Error: Could not write to file '{output_filename}'. Details: {e}")