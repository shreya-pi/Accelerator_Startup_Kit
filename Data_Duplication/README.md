Of course. Based on our development process and the provided directory structure, here is a comprehensive README file for your project. This file explains the project's purpose, workflow, and the role of each file and directory.

---

# Snowflake Data Quality & Analytics Dashboard

## 1. Overview

This project provides a comprehensive, interactive web dashboard built with Streamlit to analyze data quality and run ad-hoc queries on a Snowflake data warehouse. It is designed to empower data teams to quickly assess table health, identify duplicates, and generate AI-powered documentation without writing boilerplate SQL.

*   **Cortex Complete**: Used to generate refined schema with relational mapping amongst tables based on keys. The refined schema is used further to create intelligent views that are able to handle referential integrity

The application dashboard operates in two main modes:
*   **Data Quality Dashboard**: A guided workflow for specific, pre-defined tables to analyze duplicate records, view cleaned data and generated descriptions for all columns in selected table.
*   **DMF Runner (Data Monitoring Function)**: A general-purpose tool to run a variety of pre-defined analytical functions on any table or column in the connected schema.


It also integrates directly with **Snowflake Cortex** to provide AI-generated descriptions for table columns, making data discovery and documentation seamless.

## 2. Core Features

*   **Guided DQ Analysis**: For key tables, get an instant summary of total records vs. duplicate records.
*   **Clean Data Validation**: Compare record counts between the original table and a de-duplicated "clean" view.
*   **AI-Powered Documentation**: Select any table and get detailed, AI-generated descriptions of each column's purpose and data type using Snowflake Cortex.
*   **Dynamic DMF Runner**: Run a library of ad-hoc queries (e.g., `COUNT`, `SUM`, `AVG`, `NULL COUNT`) on any table or view.
*   **Centralized Configuration**: All Snowflake connection details are stored in a single configuration file.
*   **Optimized Performance**: Leverages Streamlit's caching to minimize redundant queries and accelerate dashboard performance.

## 3. Application Workflow

The diagram below illustrates the flow of data and user interaction:

```
+-----------+       +-------------------+       +-------------------------+
|   User    | ----> |  Streamlit App    | ----> |     Snowflake DW        |
+-----------+       |     (app.py)      |       |  (Tables Desc & Views)  |
                      +-------+-----------+       +---------+-------------+
                              |                             |
                              | Reads Config                | Uses AI Model
                              v                             v
                      +-------------------+       +--------------------+
                      |   sf_config.py    |       |   Snowflake Cortex |
                      +-------------------+       +--------------------+
                              |
                              | Calls Helpers
                              v
                +----------------------------+
                |    helper_scripts/         |
                | (col_desc, dmf_definitions)|
                +----------------------------+
                              |
                              | Reads/Writes Files
                              v
                +----------------------------+
                |  input_and_output_files/   |
                |   (schema.txt, desc.txt)   |
                +----------------------------+
```

## 4. Directory and File Structure

Here is the breakdown of each file and its purpose within the project.

```
./
├── app.py                      # The main Streamlit application entry point.
├── requirements.txt            # Lists all Python libraries needed for the project.
├── sf_config.py                # Stores Snowflake connection credentials.
│
├── helper_scripts/             # Modules containing reusable helper logic.
│   ├── col_desc.py             # Handles the logic for describing table columns.
│   ├── cortex_complete.py      # A dedicated module to interface with Cortex.
│   ├── dmf_definitions.py      # Defines the library of Data Monitoring Functions (DMFs).
│   ├── log.py                  # Configures application logging.
│   └── process_output.py       #A module for formatting query results.
│
├── input_and_output_files/     # A place for file-based inputs and outputs.
│   ├── formatted_schema.md     # A human-readable Markdown file of the table schemas.
│   ├── original_schema.txt     # The raw schema DDL, used by col_desc.py.
│   ├── schema.json             # A structured JSON representation of the schema.
│   ├── table_desc.txt          # The text file where AI-generated descriptions are saved.
│   ├── views.json              # A structured JSON file defining required views.
│   └── views.md                # A Markdown file documenting the required views.
│
└── logs/                       # Directory where log files are stored.
```

### File-by-File Explanation

*   **`app.py`**: The heart of the application. It contains all the Streamlit code for the user interface, manages application state, and orchestrates calls to helper scripts and Snowflake.
*   **`requirements.txt`**: Defines the project's Python dependencies. A new user can install everything needed with `pip install -r requirements.txt`.
*   **`sf_config.py`**: **Critical Configuration File.** Stores a Python dictionary with all Snowflake connection parameters (user, password, account, database, schema, etc.). It is imported by any script that needs to connect to Snowflake.
---
*   **`logs/`**: A dedicated directory for storing runtime log files, keeping the root directory clean.
---

## 5. Setup and Installation

Follow these steps to get the dashboard running locally.

### Prerequisites

*   Python 3.8+
*   Access to a Snowflake account with a user, password, warehouse, database, and schema.
*   Permissions for the Snowflake user to `SHOW TABLES`, `DESCRIBE TABLE`, and run `SELECT` queries on the target tables.
*   Permissions to use the `SNOWFLAKE.CORTEX.COMPLETE` function.

### Installation Steps

1.  **Clone the Repository**
    ```bash
    git clone <your-repository-url>
    cd <your-repository-directory>
    ```

2.  **Create and Activate a Virtual Environment** (Recommended)
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Snowflake Connection**
    *   Open the `sf_config.py` file.
    *   Fill in your Snowflake account details (user, password, account, etc.). **Do not commit this file with real credentials to a public repository.**

5.  **Prepare Snowflake Objects**
    *   For each table you want to analyze in the "Data Quality Dashboard" (e.g., `TABLE`), you **must** create two corresponding views in your Snowflake schema:
        *   A duplicate records view: `TABLE_duplicate_record`
        *   A cleaned data view: `TABLE_clean_view`

6.  **Populate Schema File**
    *   Open `input_and_output_files/original_schema.txt`.
    *   Paste the `CREATE TABLE ...` DDL statements for the tables you wish to use with the "Describe Table Columns" feature. Separate each statement with a blank line.

## 6. How to Run the Application

Once the setup is complete, run the following command from your project's root directory:

```bash
streamlit run app.py
```

