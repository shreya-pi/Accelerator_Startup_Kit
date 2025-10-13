# streamlit_app.py (Your working code + ONLY the logo)
import streamlit as st
import pandas as pd
import traceback
import os # <-- IMPORT ADDED to check for logo file

from Data_Quality.entity_scripts.tables import TableComparer
from Data_Quality.entity_scripts.views import ViewComparer
from Data_Quality.entity_scripts.function import FunctionComparer
from Data_Quality.entity_scripts.procedures import ProcedureComparer

class DataQualityApp:
    def __init__(self):
        self.logo_path = "Data_Quality/assets/logo.png"
        self.entity_map = {"Tables": "table", "Views": "view", "Functions": "function", "Procedures": "procedure"}
        self.entity_display_opts = ["-- Select an Entity --"] + list(self.entity_map.keys())

    def initialize_tool(self, config):
        if 'dq_tool' not in st.session_state:
            try:
                st.session_state.dq_tool = DataQualityTool(config)
            except Exception as e:
                st.error("Data Quality Tool failed to initialize. Check console logs and configurations.")
                st.exception(e)
                st.stop()
        return st.session_state.dq_tool

    def show_logo(self):
        if os.path.exists(self.logo_path):
            st.sidebar.image(self.logo_path, width=150)

    def sidebar_controls(self, dq_tool_instance):
        if "selected_entity_display" not in st.session_state:
            st.session_state.selected_entity_display = self.entity_display_opts[0]

        def on_entity_change():
            if 'comparison_results' in st.session_state:
                del st.session_state['comparison_results']

        selected_entity_disp = st.sidebar.selectbox(
            "1. Entity type:",
            self.entity_display_opts,
            key="selected_entity_display",
            on_change=on_entity_change
        )

        run_disabled = True
        entity_key = None
        comp_mode = None
        sel_items = []
        if selected_entity_disp != '-- Select an Entity --':
            entity_key = self.entity_map[selected_entity_disp]
            st.sidebar.markdown("---")
            mode_disp = st.sidebar.radio(
                "2. Comparison mode:",
                ("Compare All Entities", "Compare Selected Entities"),
                key=f"rad_{entity_key}"
            )
            comp_mode = "all" if mode_disp == "Compare All Entities" else "selected"
            if comp_mode == "all":
                run_disabled = False
            else:
                items_key = f"avail_items_{entity_key}"
                if items_key not in st.session_state:
                    with st.spinner(f"Fetching list of {selected_entity_disp.lower()}..."):
                        _, items = dq_tool_instance.get_comparer_and_items(entity_key)
                        st.session_state[items_key] = items
                avail_items = st.session_state.get(items_key, [])
                if avail_items:
                    sel_items = st.sidebar.multiselect(
                        f"3. Select {selected_entity_disp.lower()}:",
                        avail_items,
                        key=f"ms_{entity_key}"
                    )
                    run_disabled = not bool(sel_items)
                    if run_disabled:
                        st.sidebar.warning(f"Select at least one {entity_key}.")
                else:
                    st.sidebar.info(f"No common {selected_entity_disp.lower()} found to compare.")
                    run_disabled = True
            st.sidebar.markdown("---")
            if st.sidebar.button("🚀 Run Comparison", disabled=run_disabled, key=f"btn_run_{entity_key}"):
                with st.spinner(f"Comparing {selected_entity_disp}..."):
                    st.session_state.comparison_results = dq_tool_instance.run(entity_key, comp_mode, sel_items)
                st.rerun()
        else:
            st.sidebar.info("Select an entity type to begin.")

    def show_metrics(self):
        if 'comparison_results' in st.session_state and st.session_state.comparison_results:
            results_list = st.session_state.comparison_results
            total_compared, uniform_entities, non_uniform_entities = 0, [], []
            if results_list and isinstance(results_list[0], dict):
                total_compared = len(results_list)
                for result in results_list:
                    entity_name = result.get("sql_name", "Unknown")
                    if result.get("is_uniform", False):
                        uniform_entities.append(entity_name)
                    else:
                        non_uniform_entities.append(entity_name)
            with st.container(border=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(label="**Total Entities Compared**", value=total_compared)
                with col2:
                    st.metric(label="✅ **Uniform Entities**", value=len(uniform_entities))
                    with st.expander(f"View {len(uniform_entities)} matching entities"):
                        if uniform_entities:
                            for entity in sorted(uniform_entities): st.write(f"- {entity}")
                        else: st.write("No uniform entities found.")
                with col3:
                    delta_val = f"-{len(non_uniform_entities)}" if len(non_uniform_entities) > 0 else None
                    st.metric(label="❌ **Non-Uniform Entities**", value=len(non_uniform_entities), delta=delta_val, delta_color="inverse")
                    with st.expander(f"View {len(non_uniform_entities)} mismatched entities"):
                        if non_uniform_entities:
                            for entity in sorted(non_uniform_entities): st.write(f"- {entity}")
                        else: st.write("No non-uniform entities found.")
            st.markdown("---")

    def show_details(self):
        st.subheader("📊 Comparison Details")
        if 'comparison_results' in st.session_state:
            results_list = st.session_state.comparison_results
            if not results_list:
                st.warning("Comparison ran, but no results were returned.")
            for item_result in results_list:
                if isinstance(item_result, dict):
                    sql_entity_name = item_result.get("sql_name", "Unknown")
                    details_list = item_result.get("details", [])
                    st.markdown(f"#### {sql_entity_name}")
                    if details_list:
                        df_display = pd.DataFrame(details_list, dtype=str)
                        def style_comparison_rows(row):
                            style_to_apply = ''
                            comparison_status = str(row.get("Comparison", "")).lower()
                            if 'same' not in comparison_status and 'exact match' not in comparison_status:
                                style_to_apply = 'background-color: #fff0f0'
                            return [style_to_apply] * len(row)
                        st.dataframe(df_display.style.apply(style_comparison_rows, axis=1).hide(axis="index"), use_container_width=True)
                    else:
                        st.write("_No detailed comparison attributes available._")
                    st.markdown("---")
        else:
            st.info("⬅️ Select comparison options from the sidebar and click 'Run Comparison'.")

    def run(self, config: dict):
        st.title("🛡️ Data Quality Dashboard")
        dq_tool_instance = self.initialize_tool(config)
        self.show_logo()
        st.sidebar.header("⚙️ Controls")
        self.sidebar_controls(dq_tool_instance)
        self.show_metrics()
        self.show_details()


# # --- DataQualityTool Class (This is your correct code and is unchanged) ---
class DataQualityTool:
    def __init__(self, config: dict):
        self.table_comparer = TableComparer(config)
        self.view_comparer = ViewComparer(config)
        self.function_comparer = FunctionComparer(config)
        self.procedure_comparer = ProcedureComparer(config)

    def get_comparer_and_items(self, entity_type: str):
        comparer, available_items = None, []
        try:
            if entity_type == 'table': comparer = self.table_comparer
            elif entity_type == 'view': comparer = self.view_comparer
            elif entity_type == 'function': comparer = self.function_comparer
            elif entity_type == 'procedure': comparer = self.procedure_comparer
            if comparer:
                available_items = comparer.get_available_items()
        except Exception as e:
            st.error(f"Error fetching available {entity_type}s: {e}")
        return comparer, available_items

    def run(self, entity_type: str, mode: str, selected_items: list = None) -> list:
        results = []
        try:
            method_map = {
                'table': (self.table_comparer.compare_all_tables, self.table_comparer.compare_specific_items),
                'view': (self.view_comparer.compare_all_views, self.view_comparer.compare_specific_items),
                'function': (self.function_comparer.compare_all_functions, self.function_comparer.compare_specific_items),
                'procedure': (self.procedure_comparer.compare_all_procedures, self.procedure_comparer.compare_specific_items),
            }
            compare_all_method, compare_selected_method = method_map[entity_type]
            items_to_compare = selected_items if mode == "selected" else None
            
            if mode == "all":
                results = compare_all_method()
            elif items_to_compare:
                results = compare_selected_method(items_to_compare)
            else:
                return []
        except Exception as e:
            st.error(f"An error occurred during the comparison run for {entity_type}/{mode}:")
            st.exception(e)
        return results

