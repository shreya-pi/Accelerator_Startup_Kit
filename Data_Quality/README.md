**Data Quality (DQ) Comparison Tool**

This repository provides an end-to-end solution for comparing data outputs across two database platforms: **SQL Server** and **Snowflake**. It automates the extraction, comparison, and reporting of differences for:

* **Tables**
* **Views**
* **Functions**
* **Stored Procedures**

Differences are captured in CSV files and summarized in interactive HTML reports.

---

## 📂 Repository Structure

```text
./
├── .gitignore
├── compare.py
├── config.py
├── log.py
├── main.py
├── README.md         ← (this file)
├── requirements.txt
├── Dq_analysis/
│   ├── Functions/
│   │   ├── Function_comparison_report.html
│   │   ├── GetFilmsByCategorydifferences.csv
│   │   └── GetTopCustomersByRentalsdifferences.csv
│   ├── Procedures/
│   │   ├── GetCustomerRentalsdifferences.csv
│   │   ├── GetInactiveCustomersdifferences.csv
│   │   ├── GetStoreRevenuedifferences.csv
│   │   ├── GetTopRentedMoviesdifferences.csv
│   │   └── Procedure_comparison_report.html
│   ├── Tables/
│   │   ├── dbo.*differences.csv          ← one per table
│   │   └── Table_comparison_report.html
│   └── Views/
│       ├── dbo.*differences.csv          ← one per view
│       └── View_comparison_report.html
├── entity_scripts/
│   ├── function.py
│   ├── procedures.py
│   ├── tables.py
│   ├── views.py
│   └── __pycache__/
└── inputs/
    ├── function_input.json
    └── procedure_input.json
└── logs/
    ├── dq_logs.log
    └── Dq_tool.log
```

---

## 🔧 Setup & Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/shreya-pi/Data_Analysis_Tool.git
   cd Data_Analysis_Tool
   ```

2. **Create and activate a Python virtual environment**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Linux/macOS
   venv\\Scripts\\activate   # Windows
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database connections**

   * Open `config.py` and provide connection details for both platforms.
   * Example (Snowflake):

     ```python
     SNOWFLAKE_CONFIG = {
         'user': 'YOUR_USER',
         'password': 'YOUR_PASSWORD',
         'account': 'YOUR_ACCOUNT',
         'warehouse': 'YOUR_WAREHOUSE',
         'database': 'MY_DB',
         'schema': 'MY_SCHEMA'
     }
     ```
   * Example (SQL Server):

     ```python
     SQL_SERVER_CONFIG = {
         'driver': '{ODBC Driver 17 for SQL Server}',
         'server': 'your_sql_server_host',
         'database': 'your_database',
         'username': 'your_username',
         'password': 'your_password'
     }
     ```

---

## 🚀 Usage

All comparison workflows are orchestrated via `main.py`. It sequentially executes:

1. **Table Comparison**
2. **View Comparison**
3. **Function Comparison**
4. **Procedure Comparison**

### Run the full comparison

```bash
python main.py
```

This will populate the `Dq_analysis/` directory with per-entity CSV diff files and an HTML summary report for each entity type.

### Entity-specific comparison

If you wish to compare only a specific entity type, you can invoke the scripts directly:

* **Tables**:

  ```bash
  python entity_scripts/tables.py
  ```
* **Views**:

  ```bash
  python entity_scripts/views.py
  ```
* **Functions** (requires `inputs/function_input.json`):

  ```bash
  python entity_scripts/function.py
  ```
* **Stored Procedures** (requires `inputs/procedure_input.json`):

  ```bash
  python entity_scripts/procedures.py
  ```

---

## 📄 File-by-File Overview

### Root Files

| File               | Purpose                                                                                    |
| ------------------ | ------------------------------------------------------------------------------------------ |
| `.gitignore`       | Specifies files and folders to ignore in Git.                                              |
| `compare.py`       | Contains the `Compare` class used to diff two pandas DataFrames and report differences.    |
| `config.py`        | Holds connection configurations for Snowflake and SQL Server.                              |
| `log.py`           | Sets up logging (to `logs/dq_logs.log` and `logs/Dq_tool.log`) for debug and audit trails. |
| `main.py`          | Entry point script. Orchestrates all comparisons (Tables, Views, Functions, Procedures).   |
| `requirements.txt` | Python dependencies.                                                                       |

### `inputs/`

* **function\_input.json**: Maps function names → test queries for SQL Server & Snowflake.
* **procedure\_input.json**: Maps stored procedure names → test execution calls for each platform.

### `entity_scripts/`

Each script here implements the comparison logic for a specific entity type:

| Script          | Description                                                              |
| --------------- | ------------------------------------------------------------------------ |
| `tables.py`     | Discovers tables, fetches data, diffs, writes CSVs & HTML report.        |
| `views.py`      | Discovers views, fetches data, diffs, writes CSVs & HTML report.         |
| `function.py`   | Reads `function_input.json`, executes tests, diffs results.              |
| `procedures.py` | Reads `procedure_input.json`, executes stored procedures, diffs results. |

> All scripts leverage `compare.py` for DataFrame diffing and `log.py` for logging.

### `Dq_analysis/`

After a run, this folder contains:

* **Tables/**: `*.differences.csv` per table + `Table_comparison_report.html`.
* **Views/**: `*.differences.csv` per view + `View_comparison_report.html`.
* **Functions/**: `*.differences.csv` per function + `Function_comparison_report.html`.
* **Procedures/**: `*.differences.csv` per procedure + `Procedure_comparison_report.html`.

These HTML reports summarize matched vs. mismatched entities and link to the detailed CSVs.

### `logs/`

* **dq\_logs.log**: Records high-level process milestones and errors.
* **Dq\_tool.log**: Detailed debug log, including executed SQL statements and timing.

---

## 📋 Reporting Output

1. **CSV Files**: For each entity (e.g., `dbo.actor_info`), differences are captured in `Dq_analysis/Tables/dbo.actor_infodifferences.csv`.
2. **HTML Reports**: Each entity type has a dashboard showing:

   * Total entities compared
   * Counts of matches vs. mismatches
   * Links to individual CSV difference files

Open these in a browser to interactively explore discrepancies.

---

## 🛠️ Extending the Tool

* **Add New Entities**: Implement a new script in `entity_scripts/` following the pattern of the existing ones.
* **Schema Filtering**: Modify the `INFORMATION_SCHEMA` queries in the scripts to include/exclude schemas.
* **Custom Comparison Logic**: Extend or override `Compare.compare_results()` to handle fuzzy matches, tolerances, or schema changes.

---

## 🤝 Contributing

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/new-comparator`)
3. Commit your changes (`git commit -m "Add new entity comparator"`)
4. Push to the branch (`git push origin feature/new-comparator`)
5. Open a Pull Request

---

## 📜 License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.
