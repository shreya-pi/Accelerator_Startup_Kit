import os
import subprocess
import re
from datetime import datetime
from SP_Migration.scripts.log import log_info, log_error

# --- Configuration ---
# IMPORTANT: Use the full, absolute path to your repository
# REPO_PATH = r"C:\Users\shreya.naik\Documents\SP_UI\SP_UI_test\converted_procedures\Output\SnowConvert" 


# REMOTE_URL = "https://github.com/shreya-pi/SP_Converted_Procs_Test_2.git"

COMMIT_MESSAGE = "Automated commit: Update files"
BRANCH_NAME = "main"


# script_dir = os.path.abspath(__file__)
# git_dir = os.path.dirname(os.path.dirname(script_dir))


class GitPublisher():
    def __init__(self, REPO_PATH, REMOTE_URL):
        """
        Initializes the GitPublisher with the repository path and remote URL.
        """
        # self.repo_path = re.sub(r'^\./([^/]+)', r'\1/', REPO_PATH)
        self.repo_path = REPO_PATH
        self.remote_url = REMOTE_URL
        self.commit_message = COMMIT_MESSAGE
        self.branch_name = BRANCH_NAME

    def run_command(self,command, working_dir, can_fail=False):
        """Runs a command in a specified directory and handles errors."""
        log_info(f"▶️  Running: {' '.join(command)}")
        try:
            result = subprocess.run(
                command,
                cwd=working_dir,
                check=True,       # Will raise an error for non-zero exit codes
                capture_output=True,
                text=True
            )
            if result.stdout:
                log_info(result.stdout)
            return True, result.stdout.strip()
        except FileNotFoundError:
            log_error(f"❌ Error: Command '{command[0]}' not found. Is Git installed and in your PATH?")
            return False, f"Command not found: {command[0]}"
        except subprocess.CalledProcessError as e:
            # If failure is expected, we don't treat it as a fatal error.
            if can_fail:
                return False, e.stderr.strip()
            log_error(f"❌ Error executing command: {' '.join(command)}")
            log_error(f"   Return Code: {e.returncode}")
            log_error(f"   Output (stdout):\n{e.stdout}")
            log_error(f"   Error Output (stderr):\n{e.stderr}")
            return False, e.stderr.strip()
        
    
    
    
    def git_publish(self):
        """
        Initializes a repo, configures the remote, and pushes all files.
        """
        if not os.path.isdir(self.repo_path):
            log_error(f"❌ Error: The specified directory '{self.repo_path}' does not exist.")
            return
        
        # Configure Git user (for deployment purposes)
        # log_info("Configuring Git user...")
        # subprocess.run(["git", "config", "user.name", "Automated Publisher"], cwd=self.repo_path, check=True)
        # subprocess.run(["git", "config", "user.email", "ci@example.com"], cwd=self.repo_path, check=True)
    
        # --- 1. Initialize Git Repository (if needed) ---
        git_dir = os.path.join(self.repo_path, ".git")
        if not os.path.isdir(git_dir):
            log_info("\n--- Initializing Git repository ---")
            success, _ = self.run_command(["git", "init"], working_dir=self.repo_path)
            if not success: return
        else:
            log_info(f"✅ Git repository already initialized at {self.repo_path}")
    
        # --- 2. Configure Remote URL (if needed) ---
        log_info("\n--- Checking for remote 'origin' ---")
        # We check if 'origin' already exists. This command can fail if no remotes exist.
        success, remotes = self.run_command(["git", "remote", "get-url", "origin"], working_dir=self.repo_path, can_fail=True)
        
        if not success:
            log_info("Remote 'origin' not found. Adding it now.")
            add_remote_cmd = ["git", "remote", "add", "origin", self.remote_url]
            success, _ = self.run_command(add_remote_cmd, working_dir=self.repo_path)
            if not success: return
        elif remotes != self.remote_url:
            log_error(f"⚠️ Warning: Remote 'origin' already exists but points to a different URL.")
            log_error(f"   Current: {remotes}")
            log_error(f"   Expected: {self.remote_url}")
            log_error("   Script will proceed, but please verify your configuration.")
        else:
            log_info("✅ Remote 'origin' is correctly configured.")
    
        # --- 3. Set the branch name for consistency ---
        # `git branch -M <name>` creates or renames the current branch. It's safe to run every time.
        log_info(f"\n--- Ensuring branch is named '{BRANCH_NAME}' ---")
        success, _ = self.run_command(["git", "branch", "-M", BRANCH_NAME], working_dir=self.repo_path)
        if not success: return

        # log_info("\n--- Resetting working directory to a clean state ---")  
        # success, _ = self.run_command(["git", "reset"], working_dir=self.repo_path)
        # if not success:
        #     log_error("❌ Reset failed. This might be due to uncommitted changes in the working directory.")
        #     log_error("Please commit or stash your changes before running this script.")
        #     return


        # --- 4. Add, Commit, and Push ---
        log_info("\n--- Staging all files ---")
        success, _ = self.run_command(["git", "add", "."], working_dir=self.repo_path)
        if not success: return
    
        # Check status to see if there's anything to commit
        log_info("\n--- Checking for changes to commit ---")
        success, status_output = self.run_command(["git", "status", "--porcelain"], working_dir=self.repo_path)
        if not status_output:
            log_info("✅ No changes to commit. Working directory is clean.")
            # Optional: You could still try a push in case the remote is behind.
            # For this script's purpose, we'll stop here if there are no local changes.
            return
            
        log_info("Changes detected. Proceeding with commit.")
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_commit_message = f"{COMMIT_MESSAGE} on {timestamp}"
        
        commit_cmd = ["git", "commit", "-m", full_commit_message]
        success, _ = self.run_command(commit_cmd, working_dir=self.repo_path)
        # A commit can fail if 'git add' staged nothing new, so we allow failure here.
        if not success:
            log_error("Commit failed, possibly because there were no new changes to commit after all.")
            # We can still attempt a push.
        
        log_info("\n--- Pushing to remote repository ---")
        # The '-u' flag sets the upstream branch, so future `git push` calls are simpler.
        # It's safe to use even if the upstream is already set.
        push_cmd = ["git", "push", "-u", "origin", BRANCH_NAME]
        success, _ = self.run_command(push_cmd, working_dir=self.repo_path)
        if not success:
            log_error("❌ Push failed. Check your authentication (SSH/PAT) and repository permissions.")
            return
    
        log_info(f"\n🚀 Successfully published all changes to '{BRANCH_NAME}' branch!")


# if __name__ == "__main__":
#     publisher = GitPublisher()
#     publisher.setup_and_publish()