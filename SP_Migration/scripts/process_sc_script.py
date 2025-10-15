import re
import os
from pathlib import Path
from SP_Migration.scripts.log import log_info,log_error


class ScScriptProcessor:
    def __init__(self, source_schema, target_schema):
        self.source_schema = source_schema
        self.target_schema = target_schema

        input_folder = "./SP_Migration/converted_procedures/Output/SnowConvert/"
        output_folder = "./SP_Migration/processed_procedures"

        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(exist_ok=True)  # Create output folder if it doesn't exist
    


    def process_sql_script(self, sql_script):
        """Processes a SQL script by applying transformations."""

        # # --- Step 0: Ensure input is a string ------------------------
        # sql_script = re.compile(
        #     r'CREATE\s+OR\s+REPLACE\s+PROCEDURE.*?\$\$.*?\$\$',
        #     flags=re.IGNORECASE|re.DOTALL
        # )
    
        # --- Step 1: Remove comments ----------------------------------
        sql_script = re.sub(r'--.*', '', sql_script)
        sql_script = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.DOTALL)
    
        # --- Step 2: Rename schema -----------------------------------
        sql_script = re.sub(
            rf"\b{self.source_schema}\b",
            self.target_schema,
            sql_script,
            flags=re.IGNORECASE
        )
        sql_script = re.sub(r'\$\$', '', sql_script)
    
        # --- Step 3: Drop EWI markers + next nonblank line -----------
        lines = sql_script.splitlines()
        out, skip_next_non_blank = [], False
        for L in lines:
            if '!!!RESOLVE EWI!!!' in L:
                skip_next_non_blank = True
                continue
            if skip_next_non_blank:
                if L.strip():
                    skip_next_non_blank = False
                    continue
            out.append(L)
        sql_script = "\n".join(out)
    
        # # --- Step 4: Inside each procedure, quote tables & columns ---
        # SQL_KEYWORDS = {
        #     'select','from','where','join','on','and','or','order','by',
        #     'group','into','update','merge','delete','as','return','table'
        # }
    
        def quote_ids(sql_script):
            # sql_script = re.sub(r'CREATE PROCEDURE', 'CREATE OR REPLACE PROCEDURE', sql_script, flags=re.IGNORECASE)
            # sql_script = re.sub(r'\[(\w+)\]', r'"\1"', sql_script) # Replace square brackets with double quotes
            # sql_script = re.sub(r'@(\w+)', r':\1', sql_script) # Replace @ with : for parameters
            # sql_script = re.sub(r'DECLARE @(\w+)', r'LET \1;', sql_script) # Replace DECLARE with LET
            # sql_script = re.sub(r'SET @(\w+)', r'LET \1 =', sql_script) # Replace SET with LET
            # sql_script = re.sub(r'PRINT', 'RETURN', sql_script) # Replace PRINT with RETURN
            # sql_script = re.sub(r'EXEC', 'CALL', sql_script) # Replace EXEC with CALL
            # sql_script = re.sub(r'BEGIN TRANSACTION', '', sql_script) # Remove BEGIN TRANSACTION
            # sql_script = re.sub(r'COMMIT TRANSACTION', '', sql_script) # Remove COMMIT TRANSACTION
            # sql_script = re.sub(r'ROLLBACK TRANSACTION', 'ROLLBACK', sql_script) # Replace ROLLBACK TRANSACTION with ROLLBACK
            # sql_script = re.sub(r'INTO', '', sql_script)
            # sql_script = re.sub(r'CALLUTE', 'EXECUTE', sql_script)


            # 4a) Quote table names after FROM, JOIN, etc.
            tbl_pat = r'\b(FROM|JOIN|INTO|UPDATE|MERGE\s+INTO|DELETE\s+FROM)\s+((?:\w+\.)?)(\w+)\b'
            proc_text = re.sub(
                tbl_pat,
                lambda m: f"{m.group(1)} {m.group(2)}\"{m.group(3)}\"",
                proc_text,
                flags=re.IGNORECASE
            )
    
            # 4b) Quote qualified columns: alias.col or schema.table.col
            colq_pat = r'(\b[A-Za-z_][A-Za-z0-9_]*\.)("[A-Za-z_][A-Za-z0-9_]*"|\b[A-Za-z_][A-Za-z0-9_]*\b)'
            proc_text = re.sub(
                colq_pat,
                lambda m: f"{m.group(1)}\"{m.group(2).strip('\"')}\"",
                proc_text
            )
    


            # # 4c) Quote _unqualified_ columns in SELECT, WHERE, ORDER BY, etc.
            # def repl_col(m):
            #     name = m.group(1)
            #     if (name.upper() in SQL_KEYWORDS) or name.startswith('"'):
            #         return name
            #     return f"\"{name}\""
            # proc_text = re.sub(r'\b([A-Za-z_][A-Za-z0-9_]*)\b', repl_col, proc_text)
    

            return sql_script
        

        # sql_script = re.sub(r'CREATE PROCEDURE', 'CREATE OR REPLACE PROCEDURE', sql_script, flags=re.IGNORECASE)
        # sql_script = re.sub(r'\[(\w+)\]', r'"\1"', sql_script) # Replace square brackets with double quotes
        # sql_script = re.sub(r'@(\w+)', r':\1', sql_script) # Replace @ with : for parameters
        # sql_script = re.sub(r'DECLARE @(\w+)', r'LET \1;', sql_script) # Replace DECLARE with LET
        # sql_script = re.sub(r'SET @(\w+)', r'LET \1 =', sql_script) # Replace SET with LET
        # sql_script = re.sub(r'PRINT', 'RETURN', sql_script) # Replace PRINT with RETURN
        # sql_script = re.sub(r'EXEC', 'CALL', sql_script) # Replace EXEC with CALL
        # sql_script = re.sub(r'BEGIN TRANSACTION', '', sql_script) # Remove BEGIN TRANSACTION
        # sql_script = re.sub(r'COMMIT TRANSACTION', '', sql_script) # Remove COMMIT TRANSACTION
        # sql_script = re.sub(r'ROLLBACK TRANSACTION', 'ROLLBACK', sql_script) # Replace ROLLBACK TRANSACTION with ROLLBACK
        # sql_script = re.sub(r'INTO', '', sql_script)
        # sql_script = re.sub(r'CALLUTE', 'EXECUTE', sql_script)
        # # sql_script = re.sub(r'DW_DATAACCESS', 'TEST_DB', sql_script) # Replace DW_DATAACCESS with TEST_DB
        # # sql_script = re.sub(r'\bdbo\b', 'TEST_SCHEMA', sql_script)
        # # sql_script = re.sub(r'\bEP\b', 'TEST_SCHEMA', sql_script)



    
        # Apply only to Snowflake‐style $$…$$ procedures
        proc_block = re.compile(
            r'CREATE\s+OR\s+REPLACE\s+PROCEDURE.*?\$\$.*?\$\$',
            flags=re.IGNORECASE|re.DOTALL
        )
        # sql_script = proc_block.sub(lambda m: quote_ids(m.group(0)), sql_script)


        return sql_script
    

    

    def process_all_files(self):
        """Processes all SQL files in the input folder and saves the processed versions."""
        for sql_file in self.input_folder.glob("*.sql"):
            with sql_file.open("r", encoding="utf-8-sig") as file:
                sql_script = file.read()

            processed_sql = self.process_sql_script(sql_script)

            # Save the processed SQL script
            output_file_path = self.output_folder / f"processed_{sql_file.name}"
            with output_file_path.open("w", encoding="utf-8") as output_file:
                output_file.write(processed_sql)

            log_info(f"Processed: {sql_file.name} → {output_file_path.name}")





