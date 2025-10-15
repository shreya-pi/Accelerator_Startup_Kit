# --- START OF FILE home.py (MODIFIED) ---

import streamlit as st
from Json_Parser.app import JsonToSnowflakeApp
from Hql_scripts_conversion.app import HqlScriptsConverterApp
from Data_Duplication.app import DataDuplicatesApp
from Data_Quality.app import DataQualityApp
from Teradata_Migration.app import TeradataMigrationApp
from SP_Migration.app import SPMigrationApp

# --- Page Configuration ---
st.set_page_config(
    page_title="Project Portfolio",
    page_icon="👋",
    layout="wide"
)

# --- App Instantiation and State Management ---

# Store app instances in a dictionary for easier management
if 'apps' not in st.session_state:
    st.session_state.apps = {
        'json_parser': JsonToSnowflakeApp(),
        'hql_converter': HqlScriptsConverterApp(),
        'data_duplicate': DataDuplicatesApp(),
        'data_quality': DataQualityApp(),
        'teradata_migration': TeradataMigrationApp(),
        'sp_migration': SPMigrationApp()
    }

# Initialize session state for navigation and configuration
if 'page' not in st.session_state:
    st.session_state.page = 'home'
if 'app_config' not in st.session_state:
    st.session_state.app_config = None


# --- Navigation Logic ---

def navigate_to(page_name):
    """
    Changes the page in session state.
    Resets the state of the target app component for a fresh UI.
    """
    current_page = st.session_state.page
    
    # Only reset if navigating to a *different* sub-app page
    if page_name != 'home' and page_name != current_page:
        target_app = st.session_state.apps.get(page_name)
        # Check if the app has a 'reset_state' method before calling
        if target_app and hasattr(target_app, 'reset_state') and callable(getattr(target_app, 'reset_state')):
            target_app.reset_state()
    
    st.session_state.page = page_name

# --- Sidebar for Global Configuration ---
with st.sidebar:
    if st.session_state.page != 'home':
        st.button("⬅️ Back to Home", on_click=navigate_to, args=('home',), use_container_width=True)
        st.markdown("---")


    st.title("⚙️ Configuration Details")
    st.markdown("Upload your `config.py` file to provide database connection details for required application.")

    with st.expander("View Sample `config.py` Template"):
        # Make sure you have this file in an 'assets' folder
        try:
            with open('assets/config_template.py', 'r') as file:
                config_template = file.read()
            st.code(config_template, language='python')
        except FileNotFoundError:
            st.warning("`assets/config_template.py` not found. Cannot display template.")

    uploaded_file = st.file_uploader(label="Upload `config.py`", type=["py"], label_visibility="visible")

    if uploaded_file:
        try:
            config_code = uploaded_file.read().decode("utf-8")
            # Execute the code in a temporary namespace
            ns = {}
            exec(config_code, {}, ns)
            
            # Store the relevant config dictionaries in session state
            st.session_state.app_config = {
                "SNOWFLAKE_CONFIG": ns.get("SNOWFLAKE_CONFIG"),
                "SQL_SERVER_CONFIG": ns.get("SQL_SERVER_CONFIG"),
                "TERADATA_CONFIG": ns.get("TERADATA_CONFIG") # Add others as needed
            }
            st.success("Config loaded successfully!")
        except Exception as e:
            st.error(f"Failed to parse config: {e}")
            st.session_state.app_config = None
    
    # Show status of the config
    if st.session_state.app_config:
        st.info("Configuration is loaded and ready.")
    else:
        st.warning("Awaiting configuration file upload.")



# --- Page Rendering Functions ---

def render_home_page():
    """Renders the main landing page with project blocks."""
    st.title("Welcome to SMART-π! 👋")
    # st.write("Below are some of the mini-apps I've built. Click on any of them to explore.")
    st.write("---")
    # Your column layout code here... (no changes needed)
    col1, col2 = st.columns(2, gap="large")

    with col1:
        with st.container(border=True):
            st.header("Teradata-Snowflake Migrator")
            st.write("A Streamlit app that facilitates the migration of tables from Teradata to Snowflake.")
            st.button("Explore Teradata to Snowflake Migrator", on_click=navigate_to, args=('teradata_migration',), key="proj_a")

        st.write("\n\n")

        with st.container(border=True):
            st.header("Data Duplicates App")
            st.write("A Streamlit app that identifies and manages duplicate records in Snowflake tables")
            st.button("Explore Data Duplicates App", on_click=navigate_to, args=('data_duplicate',), key="proj_c")
        st.write("\n\n")
        with st.container(border=True):
            st.header("Json Files to Snowflake Tables")
            st.write("A Streamlit app that parses JSON files from a Snowflake stage and loads them into a Snowflake table.")
            st.button("Explore Json to Snowflake Parser", on_click=navigate_to, args=('json_parser',), key="proj_e")

    with col2:
        with st.container(border=True):
            st.header("Stored Procedure Migration App")
            st.write(" A Streamlit app that facilitates the migration of stored procedures from SQL Server to Snowflake using SnowConvert")
            st.button("Explore Stored Migration App", on_click=navigate_to, args=('sp_migration',), key="proj_b")
        st.write("\n\n")
        with st.container(border=True):
            st.header("Hql Scripts Converter")
            st.write("A Streamlit app that converts HQL scripts to Snowflake-compatible SQL scripts.")
            st.button("Explore Hql Scripts Converter", on_click=navigate_to, args=('hql_converter',), key="proj_d")
        st.write("\n\n")
        with st.container(border=True):
            st.header("🤖 Data Quality App")
            st.write("A Streamlit app that compares data between Snowflake and SQL Server for quality assurance.")
            st.button("Explore Data Quality App", on_click=navigate_to, args=('data_quality',), key="proj_f")


def render_app_page(page_key, title):
    """A generic function to render any app page."""
    st.title(title)
    # st.button("⬅️ Back to Home", on_click=navigate_to, args=('home',))
    # st.write("---")

    if st.session_state.app_config and page_key != 'sp_migration':
        # Pass the config to the app's run method
        st.session_state.apps[page_key].run(config=st.session_state.app_config)
    elif st.session_state.app_config and page_key == 'sp_migration':
        # Pass the config to the app's run method
        st.session_state.apps[page_key].run()
    else:
        st.warning("Please upload a `config.py` file in the sidebar to use this application.")
        st.info("The application controls will appear here once the configuration is loaded.")


# --- Main Router Logic ---
page_routes = {
    "home": render_home_page,
    "teradata_migration": lambda: render_app_page("teradata_migration", "Teradata-Snowflake Migrator"),
    "sp_migration": lambda: render_app_page("sp_migration", "Stored Procedure Migration App"),
    "data_duplicate": lambda: render_app_page("data_duplicate", "Data Duplicates App"),
    "data_quality": lambda: render_app_page("data_quality", "Data Quality App"),
    "json_parser": lambda: render_app_page("json_parser", "Json to Snowflake Parser"),
    "hql_converter": lambda: render_app_page("hql_converter", "HQL Scripts Converter")
}

# Execute the render function for the current page
page_function = page_routes.get(st.session_state.page)
if page_function:
    page_function()
else:
    st.error("Page not found!")
    render_home_page()


