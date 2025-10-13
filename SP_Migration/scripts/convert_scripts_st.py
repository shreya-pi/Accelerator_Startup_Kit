# --- START OF FILE convert_scripts_st.py ---

import streamlit as st
from SP_Migration.scripts.convert_scripts import SnowConvertRunner # Import the refactored backend class
import re
import os
from dotenv import load_dotenv
import uuid
from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from SP_Migration.scripts.git_publisher import GitPublisher

load_dotenv()


AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
ACCOUNT_URL = os.environ.get("ACCOUNT_URL") 
AZURE_STORAGE_CONTAINER_NAME = "data" 
AZURE_STORAGE_DIRECTORY_PREFIX = "streamlit_test"
# Load environment variables from .env file

class ConvertPage:
    def __init__(self, config: dict):
        self.config = config
        self.blob_service_client = self._get_blob_service_client()
        # if 'user_id' not in st.session_state:
        #     st.session_state.user_id = f"user_{uuid.uuid4().hex[:8]}"
        self.account_url = ACCOUNT_URL
        self.user_id = st.session_state.user_id
        self.container_name = AZURE_STORAGE_CONTAINER_NAME
        self.blob_prefix = f"{AZURE_STORAGE_DIRECTORY_PREFIX}/{self.user_id}/converted_procedures/"


        # Initialize session state for this page if not already present
        if "show_analytics" not in st.session_state:
            st.session_state.show_analytics = False
        if "step_completion" not in st.session_state:
            st.session_state.step_completion = {
                'convert_procs': False
            }
        if "show_azure_files" not in st.session_state: st.session_state.show_azure_files = False
        if "viewing_file" not in st.session_state: st.session_state.viewing_file = None

    

    def _get_blob_service_client(self):
        """Initializes and returns the Azure Blob Service Client."""
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            return None
        return BlobServiceClient.from_connection_string(connection_string)



    def display_page(self):
        if st.session_state.viewing_file:
            # Create a container that acts as a modal
            with st.container(border=True):
                file_info = st.session_state.viewing_file
                st.subheader(f"📄 Viewing: `{file_info['name']}`")
                st.code(file_info['content'], language='sql', line_numbers=True)
                if st.button("❌ Close Viewer", use_container_width=True):
                    st.session_state.viewing_file = None
                    st.rerun()
            # Stop rendering the rest of the page to create the modal effect
            return


        """Renders the entire UI for the conversion component."""
        with st.container(border=True):
            st.subheader("⚙️ Run Code Conversion")
            st.markdown(
                """
                This tool converts SQL Server stored procedures to Snowflake using the SnowConvert
                Click the button below to start the process. The tool will:
                1.  Verify the SnowConvert command-line tool (`snowct`) is installed (and install it if missing).
                2.  Verify you have an active license (and install one from your `.env` file if needed).
                3.  Run the conversion on all files in the `extracted_procedures` directory.
                """
            )
            if st.button("▶️ **Run Conversion Process**", type="primary", use_container_width=True):
                self.run_conversion_workflow()

    #     if st.session_state.get("show_analytics"):
    #         st.markdown("---")
    #         with st.container(border=True):
    #             st.subheader("📊 Conversion Analytics")
    #             self.display_analytics_dashboard()


        # Action buttons to view analytics or Azure files
        st.markdown("---")
        # col1, col2 = st.columns(2)
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📊 Show/Hide Conversion Analytics", use_container_width=True):
                st.session_state.show_analytics = not st.session_state.get("show_analytics", False)
        with col2:
            if st.button("☁️ Show/Hide Files in Azure", use_container_width=True):
                st.session_state.show_azure_files = not st.session_state.get("show_azure_files", False)
        # Only show the Git publish button if the user has completed the conversion step        
        with col3:
            # This button will only appear after the conversion step is marked as complete
            # coverted_procs_path = 'path/to/folder'
            # if os.path.isdir(coverted_procs_path):

            if st.session_state.step_completion.get('convert_procs', False):
                if st.button("Publish to Git", use_container_width=True, help="Push the converted files to your Git repository."):
                    try:
                        with st.spinner("Publishing files to Git repository..."):
                            # Call your predefined function
                            github_url = "https://github.com/shreya-pi/SP_Converted_Procs_Test.git"
                            converted_procs_path = './converted_procedures/Output/SnowConvert'
                            publisher = GitPublisher(converted_procs_path, github_url)
                            publisher.git_publish()
                        st.success(f"✅ Git Publish Successful")
                    except ImportError:
                        st.error("❌ 'git_publish' function not found. Ensure 'scripts/git_utils.py' exists and is correctly configured.")
                    except Exception as e:
                        st.error(f"❌ Git publish failed: {e}")
                        st.exception(e) # Show full traceback for easy debugging
        

        if st.session_state.get("show_analytics"):
            with st.container(border=True):
                st.subheader("📊 Conversion Analytics")
                self.display_analytics_dashboard()

        if st.session_state.get("show_azure_files"):
            with st.container(border=True):
                st.subheader("☁️ Files in Azure Blob Storage")
                st.caption(f"Displaying files from path: `{self.blob_prefix}`")
                self._display_blob_files()



    def _check_azure_for_files(self):
        """Checks if files already exist in the user's Azure blob path."""
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blobs = container_client.list_blobs(name_starts_with=self.blob_prefix)
        return any(blobs)
    


    def _download_from_azure(self):
        """Downloads all files from the user's Azure path to the local output directory."""
        output_dir = "./converted_procedures/Output/SnowConvert"
        os.makedirs(output_dir, exist_ok=True)
        container_client = self.blob_service_client.get_container_client(self.container_name)
        blobs_to_download = container_client.list_blobs(name_starts_with=self.blob_prefix)
        
        count = 0
        for blob in blobs_to_download:
            blob_client = container_client.get_blob_client(blob)
            local_file_name = os.path.basename(blob.name)
            download_file_path = os.path.join(output_dir, local_file_name)
            with open(download_file_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            count += 1
        return count

    def _upload_to_azure(self):
        """Uploads files from the local output directory to the user's Azure path."""
        local_dir = "./converted_procedures/Output/SnowConvert"
        container_client = self.blob_service_client.get_container_client(self.container_name)

        count = 0
        for filename in os.listdir(local_dir):
            local_file_path = os.path.join(local_dir, filename)
            if os.path.isfile(local_file_path):
                blob_name = f"{self.blob_prefix}{filename}"
                blob_client = container_client.get_blob_client(blob_name)
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                count += 1
        return count



    def run_conversion_workflow(self):
        """Orchestrates the conversion process with Azure caching."""
        log_container = st.container(border=True)
        def ui_logger(message): log_container.text(message)

        with st.status("Starting conversion workflow...", expanded=True) as status:
            try:
                status.update(label="Step 1/3: Checking Azure cache...")
                if self._check_azure_for_files():
                    status.update(label="Cache hit! Downloading existing files from Azure...")
                    count = self._download_from_azure()
                    status.update(label=f"✅ Download complete! {count} files retrieved from cache.", state="complete")
                    st.session_state.step_completion['convert_procs'] = True
                    st.toast("Retrieved converted files from Azure cache.", icon="☁️")
                else:
                    status.update(label="Cache miss. Proceeding with local conversion...")
                    runner = SnowConvertRunner(ui_logger=ui_logger)
                    
                    status.update(label="Step 1/3: Setting up SnowConvert CLI...")
                    if not runner.setup_cli(): raise Exception("Failed to set up SnowConvert CLI.")

                    status.update(label="Step 2/3: Verifying license...")
                    if not runner.setup_license(): raise Exception("Failed to set up SnowConvert license.")

                    status.update(label="Step 3/3: Converting procedures...")
                    if not runner.run_conversion(): raise Exception("The conversion command failed.")
                    
                    st.session_state.show_analytics = True
                    
                    status.update(label="Uploading results to Azure cache...")
                    upload_count = self._upload_to_azure()
                    status.update(label=f"✅ Workflow Complete! {upload_count} files converted and uploaded.", state="complete")
                    st.session_state.step_completion['convert_procs'] = True
                    st.toast("Conversion successful and results uploaded to Azure.", icon="🚀")

            except Exception as e:
                status.update(label=f"❌ Error: {e}", state="error")
        
        # st.rerun()




    def _display_blob_files(self):
        """Renders a list of files in Azure with download links."""
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blob_list = container_client.list_blobs(name_starts_with=self.blob_prefix)
            
            # Check if blob_list is empty
            files = list(blob_list)
            if not files:
                st.info("No files found in the Azure cache for this user.")
                return
            
                        # Add a header for the file list
            st.markdown("---")
            c1, c2, c3 = st.columns([4, 1, 1])
            c1.markdown("**File Name**")
            c2.markdown("**View**")
            c3.markdown("**Download**")

            for blob in files:
                col1, col2, col3 = st.columns([4, 1, 1])
                # col1, col2 = st.columns([4, 1])
                file_name = os.path.basename(blob.name)
                # col1.text(file_name)

                with col1:
                    st.text(file_name)

                with col2:
                    if st.button(f"View file", key=f"view_{blob.name}", help=f"View {file_name}", use_container_width=True):
                        # On click, read the content and store it in session state to trigger the modal
                        blob_client = self.blob_service_client.get_blob_client(self.container_name, blob.name)
                        content = blob_client.download_blob().readall().decode('utf-8')
                        st.session_state.viewing_file = {'name': file_name, 'content': content}
                        st.rerun()


                with col3:
                    # Generate a SAS token for secure, temporary download access
                    sas_token = generate_blob_sas(
                        account_name=self.blob_service_client.account_name,
                        container_name=self.container_name,
                        blob_name=blob.name,
                        account_key=self.blob_service_client.credential.account_key,
                        permission=BlobSasPermissions(read=True),
                        expiry=datetime.now(timezone.utc) + timedelta(hours=1)
                    )
                    download_url = f"{self.account_url}/{self.container_name}/{blob.name}?{sas_token}"
                    st.link_button("Download", download_url, use_container_width=True)
        
        except Exception as e:
            st.error(f"Failed to list files from Azure: {e}")



    def display_analytics_dashboard(self):
        """Renders the assessment.txt summary dashboard."""
        assessment_file_path = "logs/assessment.txt"
        if not os.path.exists(assessment_file_path):
            st.error(f"❌ Assessment file not found at `{assessment_file_path}`.")
            return

        with open(assessment_file_path, "r", encoding='utf-8') as f:
            content = f.read()

        def parse_metrics(text):
            patterns = {
                "Files": r"- Files:\s+(\d+)", "Not Generated": r"- Files Not Generated:\s+(\d+)",
                "Total LOC": r"- Total lines of code:\s+(\d+)", "Auto-Conv %": r"- Automatically converted:\s+([\d\.]+%)",
                "Time": r"- Conversion time:\s+([\d:\.]+)", "Speed (LOC/s)": r"- Conversion speed:\s+(\d+)\s+lines",
            }
            data = {name: (match.group(1) if (match := re.search(p, text, re.M)) else "N/A") for name, p in patterns.items()}
            return data

        data = parse_metrics(content)
        c1, c2, c3 = st.columns(3)
        c1.metric("Files Converted", data["Files"])
        c2.metric("Total Lines of Code", data["Total LOC"])
        c3.metric("Conversion Time", data["Time"])
        c4, c5, c6 = st.columns(3)
        c4.metric("Files Not Generated", data["Not Generated"], delta_color="inverse")
        c5.metric("Automatic Conversion", data["Auto-Conv %"])
        c6.metric("Speed (LOC/sec)", data["Speed (LOC/s)"])

        with st.expander("View Full Assessment Report"):
            st.code(content, language='text')