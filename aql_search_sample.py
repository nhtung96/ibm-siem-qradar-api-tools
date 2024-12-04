import requests
import urllib.parse
import time
import json
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    filename="filename.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s"
)

LOG = logging.getLogger(__name__)

HEADERS = {
    "SEC": "sec",
    "Version": "20.0",
    "Accept": "application/josh"
}

URL = "https://localhost"
JSON_OUTPUT_FILE = "output.json"
TXT_OUTPUT_FILE = "output.txt"
TIMEOUT = 30
TOP_NUM = 20

def load_available_ips(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data.get("available_ips", [])

def generate_ip_list(ips):
    return ",".join([f"'{ip}'" for ip in ips])

def create_query_expression(ip_list):
    return f"""
    SELECT
        sourceip,
        destinationip,
        SUM((SourcePackets + DestinationPackets)) AS 'Total Packets (Sum)',
        UniqueCount("flowType") AS 'Flow Type (Unique Count)',
        MIN("firstPacketTime") AS 'First Packet Time (Minimum)',
        MIN("endTime") AS 'Storage Time (Minimum)',
        UniqueCount("sourcePort") AS 'Source Port (Unique Count)',
        UniqueCount("destinationPort") AS 'Destination Port (Unique Count)',
        SUM("sourceBytes") AS 'Source Bytes (Sum)',
        SUM("destinationBytes") AS 'Destination Bytes (Sum)',
        SUM((SourceBytes + DestinationBytes)) AS 'Total Bytes (Sum)',
        SUM("sourcePackets") AS 'Source Packets (Sum)',
        SUM("destinationPackets") AS 'Destination Packets (Sum)',
        UniqueCount("protocolId") AS 'Protocol (Unique Count)',
        UniqueCount(APPLICATIONNAME(applicationid)) AS 'Application (Unique Count)',
        UniqueCount((IcmpType * 256 + IcmpCode)) AS 'ICMP Type/Code (Unique Count)',
        UniqueCount("sourceFlags") AS 'Source Flags (Unique Count)',
        UniqueCount("destinationFlags") AS 'Destination Flags (Unique Count)',
        UniqueCount((SourceDSCP << 2) & 0xFC + (SourcePrecedence << 5) & 0xE0) AS 'Source QoS (Unique Count)',
        UniqueCount((DestinationDSCP << 2) & 0xFC + (DestinationPrecedence << 5) & 0xE0) AS 'Destination QoS (Unique Count)',
        UniqueCount("flowSource") AS 'Flow Source (Unique Count)',
        UniqueCount("sourceASN") AS 'Source ASN (Unique Count)',
        UniqueCount("destinationASN") AS 'Destination ASN (Unique Count)',
        COUNT(*) AS 'Count'
    FROM flows10
    WHERE destinationip IN ({ip_list})
    GROUP BY sourceip
    ORDER BY "Total Packets (Sum)" DESC
    LIMIT 20
    """

def start_search(query_expression):
    encoded_query = urllib.parse.quote(query_expression)
    url = f'{URL}/api/ariel/searches?query_expression={encoded_query}'
    response = requests.post(url, headers=HEADERS, timeout=TIMEOUT, verify=False)
    if response.ok:
        LOG.info(f"Search started successfully. Search ID: {response.json().get('search_id')}")
    else:
        LOG.error(f"Failed to start search. Status Code: {response.status_code} - {response.text}")
    return response.json().get('search_id') if response.ok else None

def get_search_results(search_id):
    result_url = f'{URL}/api/ariel/searches/{search_id}/results'
    response = requests.get(result_url, headers=HEADERS, timeout=TIMEOUT, verify=False)
    if response.ok:
        LOG.info(f"Search results fetched successfully for Search ID: {search_id}")
    else:
        LOG.error(f"Failed to fetch results for Search ID: {search_id}. Status Code: {response.status_code} - {response.text}")
    return response.json() if response.ok else {}

def process_results(json_data):
    return [flow["sourceip"] for flow in json_data.get("flows", [])[:TOP_NUM]]

def save_output(output, json_file_path, txt_file_path):
    with open(json_file_path, "w") as json_file:
        json.dump({"output": output}, json_file, indent=4)
    with open(txt_file_path, "w") as txt_file:
        txt_file.write("\n".join(output))
    LOG.info(f"Top {TOP_NUM} source IPs saved to {json_file_path} and {txt_file_path}")

def main():
    ips = load_available_ips('available_ips.json')
    ip_list = generate_ip_list(ips)
    query_expression = create_query_expression(ip_list)
    
    LOG.info("Starting search...")
    search_id = start_search(query_expression)
    if not search_id:
        LOG.error("Search failed. Exiting.")
        return
    
    LOG.info(f"Waiting for results. Search ID: {search_id}")
    time.sleep(3)
    
    json_data = get_search_results(search_id)
    if not json_data:
        LOG.error("No results found. Exiting.")
        return
    
    output = process_results(json_data)
    save_output(output, JSON_OUTPUT_FILE, TXT_OUTPUT_FILE)

if __name__ == "__main__":
    main()
