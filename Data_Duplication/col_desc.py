import os
import re
import numpy as np
import snowflake.connector
# from snowflake.cortex import Complete
from sentence_transformers import SentenceTransformer
from snowflake.connector.errors import ProgrammingError
from sklearn.metrics.pairwise import cosine_similarity
# from dotenv import load_dotenv
from Data_Duplication.log import log_info, log_error
# from config import SNOWFLAKE_CONFIG

# --- PROMPT TEMPLATE FOR SNOWFLAKE CORTEX ---
# This is the prompt you provided, ready to be filled.
CORTEX_PROMPT_TEMPLATE = """
#####
For the given table, provide a clear and concise description of each column, explaining:

The purpose of the column
The type of data it holds
Its role in the table (e.g., identifier, foreign key, status flag, timestamp, etc.)

If applicable, also mention:
Any relationships with other tables (e.g., foreign key references)
Whether the column is part of a primary key or unique constraint

{filtered_schema}
#####
"""

class SnowflakeSchemaDescriber:
    """
    A class to filter a relevant table schema from a text block and use
    Snowflake Cortex to generate a description of its columns.
    """
    def __init__(self, config):
        self.SNOWFLAKE_CONFIG = config['SNOWFLAKE_CONFIG']
        """
        Initializes the embedding model and the Snowflake connection.
        """
        log_info("Initializing...")
        # Load environment variables from .env file
        # load_dotenv()
        
        # 1. Initialize the sentence transformer model for embeddings
        log_info("Loading embedding model (all-MiniLM-L6-v2)...")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # 2. Establish connection to Snowflake
        try:
            log_info("Connecting to Snowflake...")
            sf_cfg = self.SNOWFLAKE_CONFIG
            self.conn = snowflake.connector.connect(
                user=sf_cfg['user'],
                password=sf_cfg['password'],
                account=sf_cfg['account'],
                warehouse=sf_cfg['warehouse'],
                database=sf_cfg['database'],
                schema=sf_cfg['schema']
            )
            log_info("Snowflake connection successful.")
        except Exception as e:
            log_error(f"Error connecting to Snowflake: {e}")
            self.conn = None
            raise

    def score_schema(self, schema_str: str, input_params: list) -> int:
        """
        Scores a schema based on how many input parameters it contains.
        This helps re-rank semantically similar schemas for precision.
        """
        score = 0
        for param in input_params:
            # Case-insensitive matching for robustness
            if param.lower() in schema_str.lower():
                score += 1
        return score



    def filter_schema(self, table_name: str, schema_text: str) -> str:
        """
        Embeds and filters the most relevant table schema from a larger text
        based on the user-provided table name.
        
        Args:
            table_name (str): The name of the table to search for.
            schema_text (str): A string containing multiple table schemas,
                               separated by '[SEP]'.
        
        Returns:
            str: The most relevant schema chunk.
        """
        log_info(f"\nSearching for schema matching '{table_name}'...")
        
        # Split the schema text into individual table schema chunks
        schema_chunks = [chunk.strip() for chunk in re.split(r'\n\s*\n', schema_text) if chunk.strip()]
        # schema_chunks = [chunk.strip() for chunk in schema_text.split("[SEP]") if chunk.strip()]
        if not schema_chunks:
            raise ValueError("Schema text is empty or not formatted correctly with '[SEP]'.")

        # Encode all schema chunks into vector embeddings
        schema_embeddings = self.model.encode(schema_chunks)
        
        # Encode the user's input table name. Using a list of keywords for flexibility.
        input_keywords = [table_name]
        query_embedding = self.model.encode([" ".join(input_keywords)])
        
        # Compute cosine similarity between the user input and each schema chunk
        similarities = cosine_similarity(query_embedding, schema_embeddings)[0]
        
        # Get the top 5 most semantically similar schemas
        top_n = min(5, len(schema_chunks))
        top_indices_semantic = np.argsort(similarities)[-top_n:][::-1]
        
        # Create a list of candidate schemas from the top semantic matches
        candidate_schemas = [schema_chunks[i] for i in top_indices_semantic]
        
        # Re-rank these candidates based on direct keyword coverage for higher precision
        ranked_schemas = sorted(
            candidate_schemas, 
            key=lambda schema: self.score_schema(schema, input_keywords), 
            reverse=True
        )
        
        # The best schema is the one with the highest score after re-ranking
        best_schema = ranked_schemas[0]
        
        log_info("\n--- Most Relevant Schema Found ---")
        log_info(best_schema)
        log_info("--------------------------------\n")
        
        return best_schema


    def describe_with_cortex(self, filtered_schema: str, model) -> str:
        """
        Sends a request to Snowflake Cortex to describe the columns of the
        provided schema.
        
        Args:
            filtered_schema (str): The schema text of the table to be described.
        
        Returns:
            str: The description generated by Snowflake Cortex.
        """
        if not self.conn:
            return "Cannot describe table: No Snowflake connection."
            
        log_info("Sending request to Snowflake Cortex to describe table...")
        
        # Format the final prompt using the template and the filtered schema
        final_prompt = CORTEX_PROMPT_TEMPLATE.format(filtered_schema=filtered_schema)

        sql_command = "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s)"
        params = (model, final_prompt)
        try:
            # Use the Snowflake Cortex `Complete` function
            log_info(f"\nExecuting prompt on Cortex with model '{model}'...")
            cursor = self.conn.cursor()
            cursor.execute(sql_command, params)
            result = cursor.fetchone()
        
            if result:
                return result[0]
            else:
                return "Error: No response received from Snowflake Cortex."


            # response = Complete('snowflake-llama-3.3-70b', final_prompt, self.conn)
            # print("Cortex response received.")
            # return response
        except ProgrammingError as e:
            log_error(f"Snowflake Programming Error: {e}")
            return f"Error executing query. Does the role have USAGE on the SNOWFLAKE.CORTEX functions? Details: {e}"
        
        except Exception as e:
            log_error(f"Error calling Snowflake Cortex: {e}")
            return "Failed to get description from Snowflake Cortex."

    def close_connection(self):
        """Closes the Snowflake connection if it's open."""
        if self.conn and not self.conn.is_closed():
            self.conn.close()
            log_info("\nSnowflake connection closed.")

# --- Main Execution Block ---
if __name__ == "__main__":
    
    # Example multi-table schema text. In a real application, this might be
    # read from a file or fetched from a metadata store.
    with open('formatted_schema', 'r', encoding='utf-8') as file:
        SCHEMA_TEXT_BLOB = file.read()
     
    
    describer = None
    try:
        # 1. Initialize the describer (connects to Snowflake, loads model)
        describer = SnowflakeSchemaDescriber()
        
        # 2. Get user input for the table name
        # user_table_name = input("Enter the name of the table you want to describe (e.g., 'customers', 'orders'): ")
        user_table_name = "CUSTOMERS"  # Example table name, replace with user input as needed
        
        # 3. Filter out the most relevant schema
        filtered_schema = describer.filter_schema(user_table_name, SCHEMA_TEXT_BLOB)
        
        # 4. Send to Cortex and get the description
        description = describer.describe_with_cortex(filtered_schema, model = 'snowflake-llama-3.3-70b')
        
        # # 5. Print the final result
        # print("\n\n--- Snowflake Cortex Analysis ---")
        # print(f"Description for table '{user_table_name.upper()}':\n")
        # print(description)
        # print("---------------------------------")

        output_filename = "table_desc.txt"
        try:
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(description)
            log_info(f"\n✅ Response successfully saved to '{output_filename}'")
        except IOError as e:
            log_error(f"\n❌ Error: Could not write to file '{output_filename}'. Details: {e}")
        
    except Exception as e:
        log_error(f"\nAn error occurred: {e}")
    finally:
        # Ensure the connection is always closed
        if describer:
            describer.close_connection()