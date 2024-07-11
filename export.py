import os
import subprocess
import sys
import yaml
import datetime
import threading
import time

# Check if source kubeconfig file is provided
if len(sys.argv) < 2:
    print("Usage: python3 export_k8s_resources.py <source-kubeconfig>")
    sys.exit(1)

SOURCE_KUBECONFIG = sys.argv[1]

print(f"Using source kubeconfig file: {SOURCE_KUBECONFIG}")

# Exclude namespaces
EXCLUDED_NAMESPACES = ["kube-system", "monitoring", "logging", "ingress-nginx", "velero", "kube-public", "kube-node-lease"]

# Directory to save exported YAMLs
EXPORT_DIR = "exported_resources"
LOG_DIR = "export_logs"
os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Check if kubectl is installed
KUBECTL_PATH = subprocess.run(["which", "kubectl"], capture_output=True, text=True).stdout.strip()
if not os.path.isfile(KUBECTL_PATH) or not os.access(KUBECTL_PATH, os.X_OK):
    print("kubectl is not installed or not found in the PATH. Please install kubectl and try again.")
    sys.exit(1)

# Get all namespaces from source cluster
result = subprocess.run([KUBECTL_PATH, "--kubeconfig", SOURCE_KUBECONFIG, "get", "namespaces", "-o", "jsonpath={.items[*].metadata.name}"], capture_output=True, text=True)
NAMESPACES = result.stdout.split()

# Function to check if a namespace is excluded
def is_excluded(ns):
    return ns in EXCLUDED_NAMESPACES or ns.startswith("gitlab-kas")

# List of resources to export (excluding unnecessary ones)
RESOURCES = ["deployments", "services", "secrets", "persistentvolumeclaims", "configmaps", "statefulsets", "daemonsets", "ingresses", "cronjobs", "jobs", "persistentvolumes"]

# Function to remove clusterIP fields from ClusterIP services
def remove_cluster_ip(yaml_content):
    try:
        data = yaml.safe_load(yaml_content)
        if isinstance(data, dict):
            if data.get("kind") == "Service" and data.get("spec", {}).get("type", "ClusterIP") == "ClusterIP":
                data["spec"].pop("clusterIP", None)
                data["spec"].pop("clusterIPs", None)
        elif isinstance(data, list):
            for item in data:
                if item.get("kind") == "Service" and item.get("spec", {}).get("type", "ClusterIP") == "ClusterIP":
                    item["spec"].pop("clusterIP", None)
                    item["spec"].pop("clusterIPs", None)
        return yaml.dump(data)
    except yaml.YAMLError as exc:
        print(f"Error processing YAML: {exc}")
        return yaml_content

# Function to log export process
def log_export(file, content):
    with open(file, 'a') as log:
        log.write(content + "\n")

# Export resources for each namespace
log_file = os.path.join(LOG_DIR, f"export_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.log")
for ns in NAMESPACES:
    if is_excluded(ns):
        print(f"Skipping namespace {ns}")
        log_export(log_file, f"Skipping namespace {ns}")
        continue

    print(f"Exporting resources from namespace {ns}")
    for resource in RESOURCES:
        export_file = f"{EXPORT_DIR}/{ns}-{resource}.yaml"
        get_cmd = [KUBECTL_PATH, "--kubeconfig", SOURCE_KUBECONFIG, "get", resource, "-n", ns, "-o", "yaml"]
        result = subprocess.run(get_cmd, capture_output=True, text=True)
        yaml_content = result.stdout
        if resource == "services":
            yaml_content = remove_cluster_ip(yaml_content)
        with open(export_file, 'w') as f:
            f.write(yaml_content)

        if os.path.getsize(export_file) > 0:
            print(f"Exported {resource} from namespace {ns}")
            log_export(log_file, f"Exported {resource} from namespace {ns}")
        else:
            print(f"Skipping empty file {export_file}")
            log_export(log_file, f"Skipping empty file {export_file}")

# Export PersistentVolumes (these are not namespaced)
pv_export_file = f"{EXPORT_DIR}/persistentvolumes.yaml"
get_cmd = [KUBECTL_PATH, "--kubeconfig", SOURCE_KUBECONFIG, "get", "persistentvolumes", "-o", "yaml"]
result = subprocess.run(get_cmd, capture_output=True, text=True)
with open(pv_export_file, 'w') as f:
    f.write(result.stdout)

if os.path.getsize(pv_export_file) > 0:
    print("Exported PersistentVolumes")
    log_export(log_file, "Exported PersistentVolumes")
else:
    print(f"Skipping empty file {pv_export_file}")
    log_export(log_file, f"Skipping empty file {pv_export_file}")

print("Export completed. Check logs for details.")