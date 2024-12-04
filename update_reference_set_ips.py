import requests
import time
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    filename="filename.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s"
)

LOG = logging.getLogger(__name__)

IPS_COLLECTION_ID = 1000
IPS_FILE = "available_ips.txt"
QRADAR_HOST = "localhost"
TIMEOUT = 30
MAX_RETRIES = 5
HEADERS  = {
    "SEC": "sec",
    "Version": "20.0",
    "Accept": "application/json",
    'Content-Type': 'application/json'
}

def read_ips_from_file(file_path):
    with open(file_path, "r") as file:
        return [line.strip() for line in file if line.strip()]

def empty_reference_set(reference_set_id):
    data = { "delete_entries": True }
    url = f'{QRADAR_HOST}/api/reference_data_collections/sets/{reference_set_id}'
    response = requests.post(url, json=data, headers=HEADERS, timeout=TIMEOUT, verify=False)
    
    if response.ok:
        LOG.info(f"Emptied the reference set {reference_set_id}.")
    else:
        LOG.info(f"Failed to empty reference set: {response.status_code}, {response.text}")
        return None

def add_ips_to_reference_set(reference_set_id, ip_list):
    if not ip_list:
        LOG.info("No IPs to add.")
        return
    data = [{"collection_id": reference_set_id, "value": ip} for ip in ip_list]
    url = f'{QRADAR_HOST}/api/reference_data_collections/set_entries'
    response = requests.patch(url, json=data, headers=HEADERS, timeout=TIMEOUT, verify=False)

    if response.ok:
        LOG.info(f"Added {len(ip_list)} IPs to the reference set {reference_set_id}.")
        task_id = response.json()['id']
        LOG.info(f"Task ID for add IPs to reference set: {task_id}")
        return task_id
    else:
        LOG.info(f"Failed to add IPs: {response.status_code}, {response.text}")
        return None

def check_task_status(task_id, task_type):
    if task_type == 'add':
        url = f'{QRADAR_HOST}/api/reference_data_collections/set_bulk_update_tasks/{task_id}'
    else:
        LOG.info("Invalid task type.")
        return None

    response.ok = requests.get(url, headers=HEADERS, verify=False)
    if response:
        status = response.json()['status']
        LOG.info(f"Task status for {task_type} operation: {status}")
        return status
    else:
        LOG.info(f"Failed to check task status: {response.status_code}, {response.text}")
        return None
      
def wait_for_task_completion(task_id, task_type, max_retries=MAX_RETRIES):
    count = 0
    while count < max_retries:
        status = check_task_status(task_id, task_type)

        if status == 'COMPLETED':
            LOG.info("Task completed successfully.")
            break
        elif status == 'FAILED':
            LOG.info("Task failed.")
            break
        else:
            LOG.info("Waiting for task to complete...")
            time.sleep(5)
            count += 1

    if count == max_retries:
        LOG.warning("Timeout reached. Task did not complete within the allowed time.")

def main():
    ip_list = read_ips_from_file(IPS_FILE)

    empty_reference_set(IPS_COLLECTION_ID)
    time.sleep(3)

    add_task_id = add_ips_to_reference_set(IPS_COLLECTION_ID, ip_list)
    if add_task_id:
        wait_for_task_completion(add_task_id, 'add')

if __name__ == "__main__":
    main()
