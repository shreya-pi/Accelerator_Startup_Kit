# compare.py
from Data_Quality.log import log_info, log_error
import pandas as pd
import os

class Compare:
    def __init__(self):
        self.all_comparison_reports_data_for_html = []

    def is_comparison_uniform(self, details: list) -> bool:
        """
        Checks if a detailed comparison result list represents a uniform entity.
        An entity is uniform if every "Comparison" value is 'Same' or 'Exact Match'.
        """
        if not details:
            return False  # No details means we can't confirm uniformity
        
        for item in details:
            comparison_status = str(item.get("Comparison", "")).lower()
            # If any comparison is NOT 'same' or an 'exact match', it's not uniform.
            if 'same' not in comparison_status and 'exact match' not in comparison_status:
                return False
        
        return True # All items were "Same" or "Exact Match"

    def _are_types_equivalent(self, sql_type: str, sf_type: str) -> bool:
        if sql_type == sf_type:
            return True

        equivalency_map = {
            'int64': ['int64', 'int32', 'int16', 'int8', 'float64'],
            'int32': ['int64', 'int32', 'int16', 'int8', 'float64'],
            'int16': ['int64', 'int32', 'int16', 'int8', 'float64'],
            'int8':  ['int64', 'int32', 'int16', 'int8', 'float64'],
            'float64': ['float64', 'float32'],
            'float32': ['float64', 'float32'],
            'bool': ['bool', 'int8', 'int16', 'int32', 'int64'],
            'object': ['object'],
            'datetime64[ns]': ['datetime64[ns]', 'object'],
        }

        if sql_type in equivalency_map:
            return sf_type in equivalency_map[sql_type]
        if sf_type in equivalency_map:
            return sql_type in equivalency_map[sf_type]
        return False

    def compare_results(self, df_sf_norm: pd.DataFrame, df_sql_norm: pd.DataFrame,
                        snowflake_name: str, sqlserver_name: str, entity_type: str) -> list:
        current_comparison_details = []
        safe_entity_type_caps = "".join(c if c.isalnum() else "_" for c in entity_type.capitalize())
        safe_sqlserver_name_for_file = "".join(c if c.isalnum() else "_" for c in sqlserver_name)

        # 1. Compare number of rows
        count_sf, count_sql = len(df_sf_norm), len(df_sql_norm)
        current_comparison_details.append({
            "Attribute": "Number of Rows", "Snowflake Output": count_sf, "SQL Server Output": count_sql,
            "Comparison": "Same" if count_sf == count_sql else "Different"
        })

        # 2. Compare column names
        cols_sf, cols_sql = set(df_sf_norm.columns), set(df_sql_norm.columns)
        sorted_common_cols = sorted(list(cols_sf))
        current_comparison_details.append({
            "Attribute": "Column Names",
            "Snowflake Output": ", ".join(sorted_common_cols),
            "SQL Server Output": ", ".join(sorted(list(cols_sql))),
            "Comparison": "Same" if cols_sf == cols_sql else "Different"
        })

        # 3. Data Type and Data Value Comparison
        if cols_sf != cols_sql:
            current_comparison_details.extend([
                {"Attribute": "Data Types", "Snowflake Output": "N/A (Schema Mismatch)", "SQL Server Output": "N/A (Schema Mismatch)", "Comparison": "Different"},
                {"Attribute": "Data Comparison", "Snowflake Output": "N/A (Schema Mismatch)", "SQL Server Output": "N/A (Schema Mismatch)", "Comparison": "Different"}
            ])
        elif df_sf_norm.empty and df_sql_norm.empty:
            current_comparison_details.extend([
                {"Attribute": "Data Types", "Snowflake Output": "Match (Both Empty)", "SQL Server Output": "Match (Both Empty)", "Comparison": "Same"},
                {"Attribute": "Data Comparison", "Snowflake Output": "Match (Both Empty)", "SQL Server Output": "Match (Both Empty)", "Comparison": "Same"}
            ])
        else:
            try:
                dtype_sf_map = {col: str(df_sf_norm[col].dtype) for col in sorted_common_cols}
                dtype_sql_map = {col: str(df_sql_norm[col].dtype) for col in sorted_common_cols}
                types_are_equivalent = all(
                    self._are_types_equivalent(dtype_sql_map.get(col), dtype_sf_map.get(col)) for col in sorted_common_cols
                )
                comparison_result = "Same" if types_are_equivalent else "Different"
                sf_type_list = [dtype_sf_map[col] for col in sorted_common_cols]
                sql_type_list = [dtype_sql_map[col] for col in sorted_common_cols]
                current_comparison_details.append({
                    "Attribute": "Data Types",
                    "Snowflake Output": ", ".join(sf_type_list),
                    "SQL Server Output": ", ".join(sql_type_list),
                    "Comparison": comparison_result
                })

                if types_are_equivalent:
                    try:
                        df_sf_casted = df_sf_norm.astype(df_sql_norm.dtypes.to_dict())
                        df_sql_reordered = df_sql_norm[sorted_common_cols]
                        
                        if df_sf_casted.equals(df_sql_reordered):
                            current_comparison_details.append({"Attribute": "Data Comparison", "Snowflake Output": "Exact Match", "SQL Server Output": "Exact Match", "Comparison": "Same"})
                        else:
                            diff_dir = f"Dq_analysis/{safe_entity_type_caps}"
                            os.makedirs(diff_dir, exist_ok=True)
                            merged_diff = df_sf_casted.merge(df_sql_reordered, how='outer', indicator=True)
                            diff_rows = merged_diff[merged_diff['_merge'] != 'both']
                            diff_rows.to_csv(f"{diff_dir}/{safe_sqlserver_name_for_file}_differences.csv", index=False)
                            log_info(f"Differences for {sqlserver_name} saved to a CSV file.")
                            current_comparison_details.append({
                                "Attribute": "Data Comparison", "Snowflake Output": f"{len(diff_rows)} row differences found",
                                "SQL Server Output": f"{len(diff_rows)} row differences found", "Comparison": "Data mismatch detected"
                            })
                    except (ValueError, TypeError) as cast_error:
                        if df_sf_norm.astype(str).equals(df_sql_norm.astype(str)):
                             current_comparison_details.append({"Attribute": "Data Comparison", "Snowflake Output": "Exact Match (as string)", "SQL Server Output": "Exact Match (as string)", "Comparison": "Same"})
                        else:
                             current_comparison_details.append({"Attribute": "Data Comparison", "Snowflake Output": f"Data mismatch, cast failed: {cast_error}", "SQL Server Output": "N/A", "Comparison": "Different"})
                else:
                    current_comparison_details.append({"Attribute": "Data Comparison", "Snowflake Output": "N/A (Data types not equivalent)", "SQL Server Output": "N/A (Data types not equivalent)", "Comparison": "Different"})
            except Exception as e:
                log_error(f"Error during detailed compare for {sqlserver_name}: {e}")
                current_comparison_details.append({"Attribute": "Data Comparison", "Snowflake Output": f"Error: {e}", "SQL Server Output": f"Error: {e}", "Comparison": "Error"})
        
        self.all_comparison_reports_data_for_html.append({
            "snowflake_name": snowflake_name, "sqlserver_name": sqlserver_name,
            "entity_type": entity_type, "details": current_comparison_details
        })
        return current_comparison_details

    def generate_comparison_html_from_structured_data(self, output_filename_base: str, entity_type_report_title: str):
        if not self.all_comparison_reports_data_for_html: return
        safe_entity_caps = "".join(c if c.isalnum() else "_" for c in entity_type_report_title.capitalize())
        report_dir = f"Dq_analysis/{safe_entity_caps}_Reports"
        os.makedirs(report_dir, exist_ok=True)
        output_filename = f"{report_dir}/{output_filename_base}_comparison_report.html"
        html_content = "..." # The HTML generation part is unchanged
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        log_info(f"HTML report generated: {output_filename}")
        self.all_comparison_reports_data_for_html = []