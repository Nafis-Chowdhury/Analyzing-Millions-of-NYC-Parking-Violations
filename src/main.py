import argparse
import sys
import os
import requests
import json
from requests.auth import HTTPBasicAuth 
from sodapy import Socrata
from math import ceil

# DATASET_ID = "nc67-uf89"
# APP_TOKEN = "zB7Lbe46bmn5Ol2ayQHKK6KUd"
# #INDEX_NAME = "violations"
# ES_HOST = "https://search-project-yzmpliomryfrkwwfq7n6hv5w4q.us-east-1.es.amazonaws.com" 
# ES_USERNAME = "naf"
# ES_PASSWORD = 'Project@123'

DATASET_ID = os.environ["DATASET_ID"]
APP_TOKEN = os.environ["APP_TOKEN"]
ES_HOST = os.environ["ES_HOST"]
INDEX_NAME = os.environ["INDEX_NAME"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]


parser = argparse.ArgumentParser(description=("Begin processing of data"))
parser.add_argument("--page_size",type=int, help="Number of rows to fetch", required=True)
parser.add_argument("--num_pages",type=int, help="Number of pages to fetch")
args = parser.parse_args(sys.argv[1:])


client = Socrata("data.cityofnewyork.us", APP_TOKEN)
data_size = client.get(DATASET_ID, select='COUNT(*)')[0]
num_rows = int(str(data_size)[11:19])

if args.num_pages == None:
    page_size = args.page_size
    num_pages = ceil(num_rows/page_size)

else:
    page_size = args.page_size
    num_pages = args.num_pages



if __name__ == '__main__':
    try:
        resp = requests.put(
            f"{ES_HOST}/{INDEX_NAME}",
            auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            
            json = {
                "settings": {
                    "number_of_shards": 1,
                },
                "mappings": {
                    "properties": {
                        "plate": {"type": "keyword"},
                        "state": {"type": "keyword"},
                        "license_type": {"type": "keyword"},
                        "summons_number": {"type": "keyword"},
                        "issue_date": {"type": "date", "format": "MM/dd/yyyy"},
                        "violation": {"type": "keyword"},
                        "fine_amount": {"type": "float"},
                        "penalty_amount": {"type": "float"},
                        "interest_amount": {"type": "float"},
                        "reduction_amount": {"type": "float"},
                        "payment_amount": {"type": "float"},
                        "amount_due": {"type": "float"},
                        "county": {"type": "keyword"}
                    }
                },
            }
            )
        resp.raise_for_status()
    except Exception:
        print("Index already exists!")

    
    es_rows =[]
    for i in range(0, num_pages):  
        rows = client.get(DATASET_ID, limit = page_size, offset=i*(page_size), order="summons_number")
        for row in rows:
            try:
                es_row = {}
                es_row["plate"] = row["plate"]
                es_row["state"] = row["state"]
                es_row["license_type"] = row["license_type"]
                es_row["summons_number"] = row["summons_number"]
                es_row["issue_date"] = row["issue_date"]
                es_row["violation"] = row["violation"]
                es_row["fine_amount"] = float(row["fine_amount"])
                es_row["penalty_amount"] = float(row["penalty_amount"])
                es_row["interest_amount"] = float(row["interest_amount"])
                es_row["reduction_amount"] = float(row["reduction_amount"])
                es_row["payment_amount"] = float(row["payment_amount"])
                es_row["amount_due"] = float(row["amount_due"])
                es_row["county"] = row["county"]
            except Exception as e:
                # print(f"FAILED! error is: {e}")
                pass
            
            es_rows.append(es_row)
            
        #print(es_rows)
        #print(len(es_rows))
        
bulk_upload_data = ""
for i, line in enumerate(es_rows):
    print(f"Handling row {line['summons_number']} {i}")
    action = '{"index": {"_index": "'+INDEX_NAME+'", "_type" : "_doc" }}' 
    data = json.dumps(line)
    bulk_upload_data += f"{action}\n"
    bulk_upload_data += f"{data}\n"
        
        #print(bulk_upload_data)
            
try:
    resp = requests.post(
    f"{ES_HOST}/_bulk",
    auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
    data=bulk_upload_data,
    headers={
        "Content-Type": "application/x-ndjson"
        }
        )
    resp.raise_for_status()
    print("done")
except Exception as e:
    print(f"Failed to upload to elasticsearch! {e}")
    
            
       