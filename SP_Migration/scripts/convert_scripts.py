
# --- START OF FILE convert_scripts.py ---

import subprocess
import os
import sys
import shutil
import platform
import tempfile
import datetime
import re
import requests
import tarfile
import zipfile
import time
from dotenv import load_dotenv

# --- Helper to load .env file ---
# def load_env_vars():
#     env_path = os.path.join(os.getcwd(), ".env")
#     if os.path.isfile(env_path):
#         try:
#             with open(env_path, "r") as f:
#                 for raw in f:
#                     line = raw.strip()
#                     if not line or line.startswith("#") or "=" not in line:
#                         continue
#                     key, val = line.split("=", 1)
#                     if key.strip() and key.strip() not in os.environ:
#                         os.environ[key.strip()] = val.strip().strip('"').strip("'")
#             print("INFO: Loaded environment variables from .env")
#         except Exception as e:
#             print(f"ERROR: Failed to load .env: {e}")

# load_env_vars()
load_dotenv()  # Load .env variables into os.environ
class SnowConvertRunner:
    def __init__(self, ui_logger=None):
        """
        Initializes the runner.
        :param ui_logger: A callback function (e.g., st.write) to send logs to the UI.
        """
        self.input_path = "./extracted_procedures"
        self.output_path = "./converted_procedures"
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs("./logs", exist_ok=True)
        self.log_file = "./logs/assessment.txt"
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w") as f:
                f.write("SnowConvert CLI Assessment Log\n")
                f.write("=" * 40 + "\n\n")
        self.ui_logger = ui_logger

        # self._setup_snowconvert_home()  


    # def _setup_snowconvert_home(self):
    #     # """
    #     # Sets the SNOWCONVERT_HOME environment variable to a writable directory
    #     # within the application's folder. This is crucial for environments like
    #     # Azure App Service where the default home directory may not be writable
    #     # or persistent.
    #     # """
    #     # snowconvert_home = os.path.abspath("./.snowconvert_home")
    #     # os.makedirs(snowconvert_home, exist_ok=True)
    #     # os.environ["SNOWCONVERT_HOME"] = snowconvert_home
    #     # self._log(f"Set SNOWCONVERT_HOME to: {snowconvert_home}")

    #     """
    #     Creates a temporary, local home directory and sets the HOME environment
    #     variable to point to it. This forces tools that write to `~/` (the home
    #     directory) to use a writable path within our application's folder.
    #     This is the definitive fix for permission errors in container environments.
    #     """
    #     # Define a path inside our app folder. This is guaranteed to be writable.
    #     local_home_path = os.path.abspath("./temp_home")
    #     os.makedirs(local_home_path, exist_ok=True)
        
    #     # Set the HOME environment variable for this script's process and its children.
    #     # os.environ['HOME'] = local_home_path
        
    #     self._log(f"Redefined HOME directory for this session to: {local_home_path}")
    #     self._log("All CLI operations will now use this local, writable directory.")



    def _log_directory_contents(self, directory_path: str):
        """
        A helper to list and log the contents of a directory for debugging purposes.
        Uses 'ls -la' for detailed output in a Linux environment.
        """
        self._log(f"--- Listing contents of directory: {directory_path} ---")
        if not os.path.isdir(directory_path):
            self._log(f"Directory does not exist: {directory_path}", "WARN")
            return

        try:
            # 'ls -la' gives a detailed, long-format listing including hidden files.
            command = ["ls", "-la", directory_path]
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            # Log the full output, which will show permissions, owner, and file names.
            self._log(f"\n{result.stdout.strip()}")
        except Exception as e:
            self._log(f"Failed to list contents of {directory_path}: {e}", "ERROR")

# Add this method inside the SnowConvertRunner class

    def _set_permissions_dangerously(self, path: str):
        """
        Sets permissions of a given path to 777 (read/write/execute for all).
        WARNING: This is a major security risk and should only be used for
        temporary debugging to diagnose a stubborn permissions issue.
        """
        self._log(f"DANGEROUS: Setting permissions of '{path}' to 777...", "WARN")
        if not os.path.exists(path):
            self._log(f"Path does not exist, cannot set permissions: {path}", "ERROR")
            return
    
        try:
            # The '0o' prefix indicates an octal number, which is required for chmod.
            command = ["chmod", "-R", "777", path]
            subprocess.run(command, check=True)
            # os.chmod(path, 0o777)
            self._log(f"Successfully set permissions on '{path}'.")
        except Exception as e:
            self._log(f"Failed to set permissions on '{path}': {e}", "ERROR")

    # --- NEW HELPER METHOD TO DELETE A FILE ---
    def _delete_file_safely(self, file_path: str) -> bool:
        """
        Safely deletes a single file at the given path.

        - Checks if the path exists.
        - Checks if the path is a file (not a directory).
        - Handles exceptions during deletion.
        - Logs the outcome.

        :param file_path: The absolute or relative path to the file.
        :return: True if the file is gone (or was never there), False on an error.
        """
        self._log(f"Attempting to delete file: {file_path}")
        try:
            # First, check if the path exists at all.
            if not os.path.exists(file_path):
                self._log("File does not exist, nothing to delete.", "INFO")
                return True # Success, because the desired state is achieved.

            # Second, confirm it's a file, not a directory.
            if not os.path.isfile(file_path):
                self._log(f"Path is a directory, not a file. Will not delete: {file_path}", "ERROR")
                return False

            # If it exists and is a file, try to remove it.
            os.remove(file_path)
            self._log(f"✅ Successfully deleted file: {file_path}")
            return True

        except Exception as e:
            self._log(f"❌ Failed to delete file '{file_path}': {e}", "ERROR")
            return False

    def _log(self, message: str, level: str = "INFO"):
        """Logs a message to the UI logger if available, otherwise prints."""
        log_message = f"[{level}] {message}"
        if self.ui_logger:
            self.ui_logger(log_message)
        else:
            print(log_message)
    
    def _error(self, message: str):
        """Logs an error message to the UI logger if available, otherwise prints."""
        log_message = f"[ERROR] {message}"
        if self.ui_logger:
            self.ui_logger(log_message)
        else:
            print(log_message, file=sys.stderr)


    def setup_cli(self):
        """Checks if 'snowct' is on PATH and attempts to install it if not."""
        self._log("Verifying SnowConvert CLI (snowct) installation...")
        if shutil.which("snowct"):
            self._log("Found existing SnowConvert CLI on PATH.")
            return True

        current_os = platform.system().lower()
        self._log(f"'snowct' not found. Detected OS: {current_os}. Attempting automatic install…")

        try:
            if current_os == "darwin": self._install_cli_macos()
            elif current_os == "linux":
                self._install_cli_linux()
            elif current_os in ("windows", "win32"): self._install_cli_windows()
            else:
                self._log(f"Automatic install only supports macOS and Windows. Detected OS: {current_os}", "ERROR")
                self._log("Please install SnowConvert CLI manually and re-run.", "ERROR")
                return False
        except Exception as e:
            self._log(f"CLI installation failed: {e}", "ERROR")
            return False

        if not shutil.which("snowct"):
            self._log("'snowct' is still not on PATH after installation. Verify installation.", "ERROR")
            return False
        
        self._log("✅ SnowConvert CLI is ready.")
        return True

    # --- NEW METHOD: _install_cli_linux ---
    def _install_cli_linux(self):
        """Downloads and installs the SnowConvert CLI for Linux (x64 or ARM64)."""
        machine = platform.machine().lower()
        if machine in ("x86_64", "amd64"):
            arch, url = "x64", "https://sctoolsartifacts.z5.web.core.windows.net/linux/prod/cli/SnowConvert-CLI-linux.tar"
        elif machine in ("arm64", "aarch64"):
            arch, url = "arm64", "https://sctoolsartifacts.z5.web.core.windows.net/linux/prod/cli/SnowConvert-CLI-arm64-linux.tar"
        else:
            raise Exception(f"Unsupported Linux architecture: {machine}")
        
        extract_to = os.path.abspath("./SnowConvert-CLI-linux")
        orchestrator_path = os.path.join(extract_to, "orchestrator")
        
        self._log(f"Downloading SnowConvert for Linux ({arch})...")
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, "snowct.tar.gz")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(tar_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
            
            self._log("Download complete. Extracting...")
            with tarfile.open(tar_path, "r:gz") as tar:
                tar.extractall(path=extract_to)

        os.environ["PATH"] = orchestrator_path + os.pathsep + os.environ.get("PATH", "")
        self._log(f"Added '{orchestrator_path}' to PATH.")
        # Make sure the binary is executable
        snowct_binary = os.path.join(orchestrator_path, "snowct")
        if os.path.exists(snowct_binary):
            os.chmod(snowct_binary, 0o777) # Set executable permission
            self._log(f"Set executable permission on {snowct_binary}")


    def _install_cli_macos(self):
        # (This method's internal logic is the same, but uses self._log)
        machine = platform.machine().lower()
        if machine in ("x86_64", "amd64"): arch, url = "x64", "https://sctoolsartifacts.z5.web.core.windows.net/darwin_x64/prod/cli/SnowConvert-CLI-mac.tar"
        elif machine in ("arm64", "aarch64"): arch, url = "arm64", "https://sctoolsartifacts.z5.web.core.windows.net/darwin_arm64/prod/cli/SnowConvert-CLI-arm64-mac.tar"
        else: raise Exception(f"Unsupported macOS architecture: {machine}")
        
        extract_to = os.path.abspath("./SnowConvert-CLI-macos")
        orchestrator_path = os.path.join(extract_to, "orchestrator")
        
        self._log(f"Downloading SnowConvert for macOS ({arch})...")
        # Download and extraction logic here (unchanged, just replace self._log/error with self._log)
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, "snowct.tar.gz")
            # ... download request with progress ...
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(tar_path, "wb") as f: f.write(r.content)
            
            self._log("Download complete. Extracting...")
            with tarfile.open(tar_path, "r:gz") as tar: tar.extractall(path=extract_to)

        os.environ["PATH"] = orchestrator_path + os.pathsep + os.environ.get("PATH", "")
        self._log(f"Added '{orchestrator_path}' to PATH.")

    def _install_cli_windows(self):
        # (This method's internal logic is the same, but uses self._log)
        machine = platform.machine().lower()
        if machine in ("amd64", "x86_64"): arch, url = "x64", "https://sctoolsartifacts.z5.web.core.windows.net/windows/prod/cli/SnowConvert-CLI-windows.zip"
        elif machine in ("arm64", "aarch64"): arch, url = "arm64", "https://sctoolsartifacts.z5.web.core.windows.net/windows/prod/cli/SnowConvert-CLI-arm64-windows.zip"
        else: raise Exception(f"Unsupported Windows architecture: {machine}")

        extract_to = os.path.abspath("./SnowConvert-CLI-windows")
        orchestrator_path = os.path.join(extract_to, "orchestrator")
        
        self._log(f"Downloading SnowConvert for Windows ({arch})...")
        # Download and extraction logic here (unchanged, just replace self._log/error with self._log)
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "snowct.zip")
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(zip_path, "wb") as f: f.write(r.content)
            
            self._log("Download complete. Extracting...")
            with zipfile.ZipFile(zip_path, "r") as z: z.extractall(extract_to)

        os.environ["PATH"] = orchestrator_path + os.pathsep + os.environ.get("PATH", "")
        self._log(f"Added '{orchestrator_path}' to PATH.")

    def setup_license(self):
        """Checks for an active license and installs one from env if needed."""
        self._log("Verifying SnowConvert license...")
        
        # def get_ac_output():
        #     proc = subprocess.run(["snowct", "show-ac"], capture_output=True, text=True, input="Yes\n")
        #     return proc.stdout
        
        def get_ac_output():
            """Helper to run 'snowct show-ac' and capture its output for logging."""
            # self._log("Running 'snowct show-ac' to check license status...")
            try:
                command = ["snowct", "show-ac"]
                # self._log(f"Executing command: {' '.join(command)}")

                proc = subprocess.run(
                    # ["snowct", "show-ac", "--config dir", os.path.abspath("./.snowconvert_home")],
                    command,
                    # capture_output=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    input="Yes\n", # For any potential prompts
                    timeout=30
                )
                # Log both stdout and stderr to see everything
                # if proc.stdout:
                #     self._log(f"--- 'show-ac' STDOUT ---\n{proc.stdout.strip()}")
                # if proc.stderr:
                #     self._log(f"--- 'show-ac' STDERR ---\n{proc.stderr.strip()}", "WARN")
                return proc.stdout
            except Exception as e:
                self._log(f"Failed to execute 'snowct show-ac': {e}", "ERROR")
                return ""
        

        def has_active_license(output:str) -> bool:
            now = datetime.datetime.now()
            self._log("🔍 Checking for active license in 'show-ac' output...")
            pattern = re.compile(r"Expiration date:\s*([0-1]?\d/[0-3]?\d/\d{4}\s+\d{1,2}:\d{2}:\d{2})")
            found = False
            # self._log(f"Searching for expiration date pattern in output:\n{output.strip()}")
            for match in pattern.finditer(output):
                date_str = match.group(1)
                try:
                    exp_dt = datetime.datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
                    # self._log(f"🔎 Found expiration date: {exp_dt}")
                    # self._log(f"Comparing with current time: {now}")
                    if exp_dt > now:
                        # self._log(f"✅ License is active (expires in future: {exp_dt})")
                        found = True
                except ValueError as ve:
                    self._log(f"⚠️ Failed to parse date: {date_str} — {ve}")
            return found
            # pattern = re.compile(r"Expiration date:\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2})")
            # now = datetime.datetime.now()
            # # for match in re.finditer(r"Expiration date:\s*(.+)", output):
            # #     try:
            # #         exp_dt = datetime.datetime.strptime(match.group(1).strip(), "%m/%d/%Y %H:%M:%S")
            # #         if exp_dt > now: return True
            # #     except ValueError: continue
            # # return False
            # for match in pattern.finditer(output):
            #     date_str = match.group(1)
            #     self._log(f"Found potential expiration date string: '{date_str}'")
            #     try:
            #         exp_dt = datetime.datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
            #         if exp_dt > now:
            #             self._log(f"✅ License is active. Expires on {exp_dt}.")
            #             return True
            #         else:
            #             self._log(f"⚠️ Found a license, but it has expired on {exp_dt}.")
            #     except ValueError:
            #         self.log(f"Could not parse date string '{date_str}'. Skipping.", "WARN")
            
            # self._log("No active license was found in the output.")
            # return False

               # Step 1: Check for existing active license
        
        # self._test_network_connectivity()

        try:
            initial_output = get_ac_output()
            # self._log("Initial 'snowct show-ac' output:")
            # self._log(initial_output.strip())
        except Exception as e:
            self._error(f"❌ Exception while getting initial access code info: {str(e)}")
            import traceback
            self._error(traceback.format_exc())
            sys.exit(1)
    
        if has_active_license(initial_output):
            # self._log("✅ Found an existing ACTIVE SnowConvert access code.")
            return True
    
        # Step 2: Try to install from environment
        self._log("❌ No active SnowConvert access code detected.")
        self._log("🔍 Checking SNOWCONVERT_ACCESS_CODE environment variable...")
    
        access_code = os.environ.get("SNOWCONVERT_ACCESS_CODE", "").strip()
        if not access_code:
            self._error("❌ Environment variable SNOWCONVERT_ACCESS_CODE is not set.")
            self._log("💡 Please set it via .env file or environment variable.")
            sys.exit(1)
    
        self._log(f"📥 Access code found in environment: {access_code[:5]}... (truncated)")


        # --- NEWLY ADDED DIAGNOSTIC STEP ---

        # snowflake_inc_dir = os.path.join(os.getcwd(), 'Snowflake Inc')
        # if os.path.isdir(snowflake_inc_dir):
        #     self._log(f"Found the '{snowflake_inc_dir}' directory.")
        #     self._set_permissions_dangerously(snowflake_inc_dir)
        #     self._log(f"Setting permissions on '{snowflake_inc_dir}' to 777 (read/write/execute for all).")
        # else:
        #     # If it's not there, maybe it gets created later.
        #     # This gives you a clue about when it's being created.
        #     self._log(f"Directory '{snowflake_inc_dir}' does not yet exist. Skipping chmod.", "INFO")
    
        new_home_dir = os.environ.get('HOME') 
        if new_home_dir:
            # Define the expected path for the .config directory
            # self._log_directory_contents(new_home_dir)
            config_dir_path = os.path.join(new_home_dir, '.config')
            # profile_file_path = os.path.join(new_home_dir, '.profile')

            # Check if the directory was created by the install command
            if os.path.isdir(config_dir_path):
            # if os.path.exists(config_dir_path):
                self._log(f"Found the '{config_dir_path}' directory. Attempting to set permissions...")
                # Call the helper to set permissions to 777
                self._set_permissions_dangerously(config_dir_path)
                self._log_directory_contents( os.path.join(new_home_dir, '.config', 'Snowflake Inc'))
                # self._delete_file_safely(os.path.join(new_home_dir, '.config', 'Snowflake Inc','.profile'))
                # self._log_directory_contents( os.path.join(new_home_dir, '.config', 'Snowflake Inc'))
            else:
                self._log(f"Expected directory '{config_dir_path}' was NOT found after installation.", "WARN")
        else:
            self._log("Could not find HOME environment variable to check for .config directory.", "WARN")


        # Step 3: Install access code
        try:
            self._log("🔧 Installing access code...")
            install_proc = subprocess.run(
                ["snowct", "install-ac", access_code],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            # self._log("✅ Access code installation command completed.")
            # self._log(f"📋 Output:\n{install_proc.stdout.strip()}")
            self._log("... 'install-ac' command executed. Verifying installation status next.")
            if install_proc.stdout:
                self._log(f"📋 Output:\n{install_proc.stdout.strip()}")
                
            if install_proc.stderr: # Also log stderr, just in case.
                self._log(f"📋 Stderr:\n{install_proc.stderr.strip()}", "WARN")

    
        except subprocess.CalledProcessError as e:
            # This handles cases where `snowct` correctly returns a non-zero exit code.
            self._error(f"❌ Error installing access code. The command failed with exit code: {e.returncode}")
            # The error message might be in stdout or stderr, so log both for context.
            if e.stdout: self._error(f"--- STDOUT ---\n{e.stdout.strip()}")
            if e.stderr: self._error(f"--- STDERR ---\n{e.stderr.strip()}")
            sys.exit(1)
        except (subprocess.TimeoutExpired, Exception) as e:
            # Catch other potential errors like timeouts during the installation.
            self._error(f"❌ An exception occurred while running 'install-ac': {e}")
            import traceback
            self._error(traceback.format_exc())
            sys.exit(1)
    


        # --- NEWLY ADDED DIAGNOSTIC STEP ---
        self._log("Running post-install diagnostics to check file system...")
        
        # Get the path of our new HOME directory from the environment variable.
        new_home_dir = os.environ.get('HOME') 
        if new_home_dir:
            # self._log_directory_contents(new_home_dir)
            self._log_directory_contents( os.path.join(new_home_dir, '.config', 'Snowflake Inc'))
        else:
            self._log("Could not find HOME environment variable to list.", "WARN")
        
        # self._log("Listing contents of the current working directory...")
        # Also list the current working directory for context.
        # self._log_directory_contents(os.getcwd())
        # self._log_directory_contents(os.path.join(os.getcwd(), 'Snowflake Inc'))

        # --- END OF DIAGNOSTIC STEP ---




        # Step 4: Re-check license status
        self._log("🔁 Re-checking license status after install...")
        try:
            post_install_output = get_ac_output()
            self._log("Post-install 'snowct show-ac' output:")
            self._log(post_install_output.strip())
        except Exception as e:
            self._error(f"❌ Failed to run 'snowct show-ac' after install: {str(e)}")
            import traceback
            self._error(traceback.format_exc())
            sys.exit(1)
    
        if not has_active_license(post_install_output):
            self._write_log("After install-ac, 'snowct show-ac' returned:\n\n" + post_install_output)
            self._error("❌ Access code did not become ACTIVE.")
            # self._log("📄 Check the logs for full 'show-ac' output.")
            # self._log("🚧 Possible issues:\n"
            #          "  • Invalid or expired access code\n"
            #          "  • Installed under different user\n"
            #          "  • Network/firewall blocking validation")
            sys.exit(1)
    
        self._log("✅ Access code installed and ACTIVE.")





    # def _test_network_connectivity(self):
    #     """
    #     Tests connectivity to essential Snowflake and SnowConvert endpoints.
    #     Returns True if all are reachable, False otherwise.
    #     """
    #     self._log("--- Starting Network Connectivity Test ---")
    #     # Endpoints needed for CLI download and license activation
    #     endpoints_to_test = {
    #         # Used for downloading the CLI itself
    #         "sctoolsartifacts.z5.web.core.windows.net": 443,
    #         # A primary endpoint for Snowflake services. If this fails, many things will.
    #         "A7088372892471-DATAFORTUNE_PARTNER.snowflakecomputing.com": 443,
    #         # Likely endpoint for license validation.
    #         # Use a generic organization URL as a stand-in for general connectivity.
    #         "api-sc-licensing-prod.azurewebsites.net/": 443
    #     }
        
    #     all_ok = True
    #     for host, port in endpoints_to_test.items():
    #         if "your_org-your_account" in host:
    #              self._log(f"Skipping placeholder URL: {host}. Please replace it with your real Snowflake URL for a complete test.", "WARN")
    #              continue
    #         try:
    #             self._log(f"Testing connection to {host}:{port}...")
    #             requests.get(f"https://{host}", timeout=15)
    #             self._log(f"✅ SUCCESS: Connection to {host} is OK.", "INFO")
    #         except requests.exceptions.RequestException as e:
    #             self._log(f"❌ FAILED: Could not connect to {host}. Reason: {e}", "ERROR")
    #             all_ok = False
                
    #     self._log("--- Network Connectivity Test Finished ---")
    #     if not all_ok:
    #         self._log("One or more required network endpoints are unreachable. This is likely the cause of the license download failure.", "ERROR")
    #         self._log("Please check your Azure App Service's outbound firewall rules (NSG, Azure Firewall, etc.) and ensure these hosts are whitelisted.", "ERROR")
    #     return all_ok



    def run_conversion(self):
        """Runs the main snowct conversion command."""
        command = ["snowct", "sql-server", "--input", self.input_path, "--output", self.output_path]
        self._log(f"Executing conversion: {' '.join(command)}")
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            self._write_log(result.stdout)
            self._log("✅ Conversion completed successfully.")
            return True
        except subprocess.CalledProcessError as e:
            self._write_log(e.stderr)
            self._log(f"Conversion failed. Check logs at {self.log_file}", "ERROR")
            return False

    def _write_log(self, content):
        with open(self.log_file, "w") as f: f.write(content)

if __name__ == "__main__":
    runner = SnowConvertRunner()
    if runner.setup_cli() and runner.setup_license():
        runner.run_conversion()


