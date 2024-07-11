import os
import subprocess
import sys
import datetime
import threading
import time

# Check if target kubeconfig file is provided
if len(sys.argv) < 2:
    print("Usage: python3 import_k8s_resources.py <target-kubeconfig>")
    sys.exit(1)

TARGET_KUBECONFIG = sys.argv[1]
MAX_THREADS = 20  # Maximum number of concurrent threads

print(f"Using target kubeconfig file: {TARGET_KUBECONFIG}")

# Directory where exported YAMLs are stored
EXPORT_DIR = "exported_resources"
LOG_DIR = "import_logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Check if kubectl is installed
KUBECTL_PATH = subprocess.run(["which", "kubectl"], capture_output=True, text=True).stdout.strip()
if not os.path.isfile(KUBECTL_PATH) or not os.access(KUBECTL_PATH, os.X_OK):
    print("kubectl is not installed or not found in the PATH. Please install kubectl and try again.")
    sys.exit(1)

# Function to apply resources and log output
def apply_resource(file, log_file, progress_callback, retries=3):
    with open(log_file, 'a') as log:
        for attempt in range(1, retries + 1):
            apply_cmd = [KUBECTL_PATH, "--kubeconfig", TARGET_KUBECONFIG, "apply", "-f", file, "--validate=false"]
            apply_result = subprocess.run(apply_cmd, capture_output=True, text=True)
            log.write(f"Apply command output for {file} (Attempt {attempt}):\n{apply_result.stdout}\n{apply_result.stderr}\n")
            if apply_result.returncode == 0:
                log.write(f"Successfully applied {file}\n")
                break
            else:
                log.write(f"Failed to apply {file} (Attempt {attempt})\n")
                if attempt < retries:
                    log.write(f"Retrying {file}...\n")
                    time.sleep(5)
                else:
                    log.write(f"Failed to apply {file} after {retries} attempts\n")
    progress_callback()

# Function to limit the number of concurrent threads
def wait_for_threads():
    while threading.active_count() > MAX_THREADS:
        time.sleep(1)

# Function to update progress
def update_progress(total, completed):
    progress = (completed / total) * 100
    print(f"Progress: {progress:.2f}% ({completed}/{total} files)")

# Define the import order
IMPORT_ORDER = [
    "namespaces",
    "configmaps",
    "secrets",
    "persistentvolumeclaims",
    "deployments",
    "statefulsets",
    "daemonsets",
    "services",
    "ingresses",
    "cronjobs",
    "jobs",
    "persistentvolumes"
]

# Import resources for each namespace and log output
log_file = os.path.join(LOG_DIR, f"import_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.log")
threads = []

# Count the total number of files to be processed
total_files = sum(1 for filename in os.listdir(EXPORT_DIR) if filename.endswith(".yaml"))
completed_files = 0
lock = threading.Lock()

def progress_callback():
    global completed_files
    with lock:
        completed_files += 1
        update_progress(total_files, completed_files)

for resource_type in IMPORT_ORDER:
    for filename in os.listdir(EXPORT_DIR):
        if filename.endswith(f"{resource_type}.yaml"):
            file_path = os.path.join(EXPORT_DIR, filename)
            if os.path.getsize(file_path) > 0:
                thread = threading.Thread(target=apply_resource, args=(file_path, log_file, progress_callback))
                thread.start()
                threads.append(thread)
                wait_for_threads()
                print(f"Started importing {filename}, see log for details.")
            else:
                print(f"Skipping empty file {filename}")

# Wait for all threads to complete
for thread in threads:
    thread.join()

print(f"Import completed. Check {log_file} for details.")