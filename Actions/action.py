import sys

import os
import json
import time
import requests
import random
import string
import csv
import base64
import io
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
CSV_CONFIGURATIONS = {
    "fields_configured": [
        {
            "csv_key": "SN",
            "mapped_field": "Name",
            "type": "Text",
        },
        # {
        #     "csv_key": "Description",
        #     "mapped_field": "Description",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "type",
        #     "mapped_field": "type",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "name",
        #     "mapped_field": "type_1",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "count",
        #     "mapped_field": "count",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "s",
        #     "mapped_field": "s",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "n",
        #     "mapped_field": "w",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "w",
        #     "mapped_field": "w_1",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "place",
        #     "mapped_field": "place",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "uses",
        #     "mapped_field": "uses",
        #     "type": "Text",
        # },
        # {
        #     "csv_key": "e",
        #     "mapped_field": "e",
        #     "type": "Text",
        # },
    ],
    "abort_on_error": False,
    "insert_only": True,
    "delete_missing": False,
}
CONFIG = {
    "HEADERS": {
        "X-Access-Key-Id": "Akc3618b44-e161-41fb-9a8e-aa3479e12c2f",
        "X-Access-Key-Secret": "svhLucmnTnZDz6wduheIAyzxIcFYvcHlvDNTuvQr1VTO8vFwJAjKUKDZfT8tcFU7v2Kdm7wujeOkgrDcbYcvQ",
    },
    "UPLOAD_HEADERS": {
        "X-Access-Key-Id": "Akc3618b44-e161-41fb-9a8e-aa3479e12c2f",
        "X-Access-Key-Secret": "svhLucmnTnZDz6wduheIAyzxIcFYvcHlvDNTuvQr1VTO8vFwJAjKUKDZfT8tcFU7v2Kdm7wujeOkgrDcbYcvQ",
        "Content-Type": "text/csv",
    },
    "ACCOUNT_ID": "Ac4Zg6muao_p",
    "DATASET_ID": "Duplicate_News_of_new_feature",
    "DOMAIN": "https://play.kissflow.com",
}

HEADERS = CONFIG["HEADERS"]
UPLOAD_HEADERS = CONFIG["UPLOAD_HEADERS"]
ACCOUNT_ID = CONFIG["ACCOUNT_ID"]
DATASET_ID = CONFIG["DATASET_ID"]
DOMAIN = CONFIG["DOMAIN"]


def generate_random_string(length=8):
    letters = string.ascii_letters
    random_string = ''.join(random.choice(letters) for _ in range(length))
    return random_string


def generate_id(prefix):
    print("Inside generate_id")
    return f"{prefix}_{generate_random_string()}"


def get_file_extension(name):
    print("Inside get_file_extension")
    if name is not None:
        if name.rfind(".") == -1:
            return "unknown"
        else:
            extension = name.split(".").pop()
            return extension.lower()


def get_file_detail(file_path):
    print("Inside get_file_detail")
    try:
        stats = os.stat(file_path)
        file_size = stats.st_size
        with open(file_path, "r", encoding="utf-8") as f:
            file_text = f.read()
        return {"file_text": file_text, "file_size": file_size}
    except OSError as e:
        print(e)


def get_file_info_from_file(lines, file_path):
    print("Inside get_file_info_from_file")
    file_info = {}
    # file_details = get_file_detail(file_path)
    # file_details = {}
    file_id = generate_id("Attach")
    file_name = os.path.basename(file_path)
    # file_details["file_text"] = lines
    file_info["key"] = f"dataset/{DATASET_ID}/{file_id}/{file_name}"
    file_info["name"] = file_name
    file_info["size"] = sys.getsizeof(lines)
    print("File info generated")
    return {"file_info": file_info, "file_name": file_name, "file_text": lines, "file_id": file_id}


def import_csv(lines, file_path="./Kissflow50k.csv"):
    print("Inside import_csv")
    print("File info generating....")
    file_info_dict = get_file_info_from_file(lines, file_path)
    print(file_info_dict)
    file_info = file_info_dict["file_info"]
    file_name = file_info_dict["file_name"]
    file_text = file_info_dict["file_text"]
    file_id = file_info_dict["file_id"]

    def get_status(import_id):
        print("Inside get_status")
        try:
            response = requests.get(
                f'{DOMAIN}/dataset/2/{ACCOUNT_ID}/{DATASET_ID}/import/{import_id}',
                headers=HEADERS
            )
            data = response.json()
            print(f"ln 166 {data}")
            if data['ImportStatus'] == 'Completed':
                print('Import csv completed')
                return
            elif data['ImportStatus'] == 'Aborted':
                print('Import csv aborted')
                return
            else:
                # print(data)
                time.sleep(3)
                print('Import csv in progress....')
                get_status(import_id)
        except Exception as e:
            print(f"Ln 179 {e}")

    def trigger_import(validate_file_payload):
        print("Inside trigger_import")
        trigger_import_payload = {'csv_file': validate_file_payload}
        trigger_import_payload.update(CSV_CONFIGURATIONS)
        try:
            print(trigger_import_payload)
            response = requests.post(
                f'{DOMAIN}/dataset/2/{ACCOUNT_ID}/{DATASET_ID}/import',
                headers=HEADERS,
                json=trigger_import_payload
            )
            data = response.json()
            print('Import csv triggered')
            print(data)
            get_status(data['ImportId'])
        except Exception as e:
            print(f" ln197 {e}")

    def validate_file():
        print("Validating file....")

        validate_file_payload = {
            "key": f"{ACCOUNT_ID}/{file_info['key']}",
            "id": file_id,
            "uploaded": 100,
            "fileExtension": get_file_extension(file_name),
        }
        validate_file_payload.update(
            {'name': file_info["name"], 'size': file_info["size"]})
        print(validate_file_payload)
        try:
            url = f"{DOMAIN}/dataset/2/{ACCOUNT_ID}/{DATASET_ID}/validate_csv"
            response = requests.post(
                url, json=validate_file_payload, headers=HEADERS)
            print("Validation completed !!!")
            trigger_import(validate_file_payload)
        except Exception as e:
            print(f" ln 218 {e}")

    def post_attachment_value(api_resp):
        print("Inside post_attachment_value")
        try:
            response = requests.put(
                api_resp["Url"], data=file_text, headers=UPLOAD_HEADERS)
            if response.status_code == 200:
                print(f"{file_name} file uploaded successfully !!")
                validate_file()
        except Exception as e:
            print(e)

    def init_upload(file_info):
        print("Init uploading....")
        try:
            url = f"{DOMAIN}/upload/2/{ACCOUNT_ID}/"
            response = requests.post(url, json=file_info, headers=HEADERS)
            print(f"Uploading {file_name}....")
            data = response.json()
            post_attachment_value(data)
        except Exception as e:
            print(e)

    init_upload(file_info)


def lambda_handler(event, context):
    # TODO implement
    print(event)
    csv_file = event['body-json']
    csv_data = csv_file.read().decode('utf-8')
    headers, data = csv_file.decode('utf-8').split('\r\n', 1)
    # memory = io.StringIO(data)
    # reader = csv.reader(memory)
    import_csv(data)
    return {"status": "success"}
# Trigger import csv need to send a path
