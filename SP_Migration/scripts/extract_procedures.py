import os
from snowflake.connector import connect, DictCursor
# from config import SNOWFLAKE_CONFIG  # import your Snowflake config
from SP_Migration.scripts.log import log_info, log_error

# Constants
# OUTPUT_DIR = "./extracted_procedures"

class ExtractProcedures:
    def __init__(self, config: dict):
        if not config or "SNOWFLAKE_CONFIG" not in config:
            log_error("Configuration is missing or invalid.")
            raise ValueError("Configuration is missing or invalid.")
        
        # Store the needed config parts as instance variables
        self.snowflake_config = config["SNOWFLAKE_CONFIG"]  
        
        # os.makedirs("./extracted_procedures", exist_ok=True)  # Ensure logs directory exists
        output_dir = "./extracted_procedures"
        self.output_dir = output_dir
        
        # 1. Ensure the output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # 2. Connect to Snowflake using config
        self.ctx = connect(
            user=self.snowflake_config['user'],
            password=self.snowflake_config['password'],
            account=self.snowflake_config['account'],
            warehouse=self.snowflake_config['warehouse'],
            database=self.snowflake_config['database'],
            schema=self.snowflake_config['schema'],
            role=self.snowflake_config['role']
        )
        self.cs = self.ctx.cursor(DictCursor)

    def extract_procedures(self):
        try:
            # 3. Query for procedures where CONVERSION_FLAG is TRUE
            self.cs.execute("""
                SELECT 
                    PROCEDURE_NAME, 
                    PROCEDURE_DEFINITION 
                FROM PROCEDURES_METADATA 
                WHERE CONVERSION_FLAG = TRUE
            """)
            rows = self.cs.fetchall()
            # 4. Write each procedure to a .sql file        
            for row in rows:
                proc_name = row["PROCEDURE_NAME"]
                definition = row["PROCEDURE_DEFINITION"]
        
                # Replace special characters in filename
                safe_name = "".join(c if c.isalnum() or c in ("_", "-") else "_" for c in proc_name)
                file_path = os.path.join(self.output_dir, f"{safe_name}.sql")
        
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(definition.strip() + "\n")
        
                log_info(f"Wrote {proc_name} → {file_path}")
                self.cs = self.ctx.cursor(DictCursor)
    
        # 5. Close the cursor and connection
        finally:
            self.cs.close()    
            self.ctx.close() 
        
    










