### File-by-File Explanation

 **`input_and_output_files/`**: Centralizes file-based data.
    *   **`original_schema.txt`**: **Crucial Input File.** This must contain the original `CREATE TABLE` DDL for all the tables in a given schema
    *   **`schema.json`**: These provide modelled schema with primary-foriegn key relationships generated using {prompt} in `cortex_complete.py`.
    *   **`formatted_schema.md`**: `schema.json` is formatted to extract relevant content using `process_output.py`. `col_desc.py` reads from this file.
    *   **`table_desc.txt`**: The default output file where the AI-generated column descriptions are saved.
    *   **`views.md` / `views.json`**: Views generated for each table(`_duplicate_record`, `_clean_view`), using {prompt_1} in `cortex_complete.py` essential to calculate and remove duplicates

