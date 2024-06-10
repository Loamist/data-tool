import streamlit as st
import os
import boto3
import json
import pandas as pd
import ijson
from datetime import datetime, timezone


aws_access_key_id = st.secrets["Access_key_ID"]
aws_default_region = st.secrets["AWS_DEFAULT_REGION"]

# get aws_secret_access_key from user input

aws_secret_access_key = st.text_input("Enter your AWS secret access key")

# wait for user to input the secret key before proceeding
if aws_secret_access_key == "":
    st.stop()


s3 =  boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_default_region
)
bucket_name = 'dev-data-layer-datasets'



def list_files_in_folder(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page['Contents']:
                files.append(obj['Key'])
    return files

folder_prefix = 'metadata/'
files = list_files_in_folder(bucket_name, folder_prefix)
json_files = [file for file in files if file.endswith('json') and file.count('/') == 1]

metadataFormat =  {
            "name": "",
            "layer_id": "",
            "geom_type": "",
            "geom_join": "",
            "description": "",
            "obj_details_column": "",
            "has_biomass": False,
            "has_county_geoid": False,
            "mandatory_filter": [],
            "human_identifier_field":"",
            "value_columns": [],
            "category_columns": [],
            "details_columns": [],
            "data_columns": [],
            "tooltip-title": [],
            "tooltip-content": "",
            "s3_file_path": "",
            "view_name": "",
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") ,
            "detials_modals": [],
            "columns": [],
            "calculated_fields": [],
            "layer_access_level": ''
        }

keys = list(metadataFormat.keys())

# select one of the keys
selected_key = st.selectbox('Select a key', keys)
def read_metadata(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    # Assuming the file is UTF-8 encoded, adjust the encoding as needed
    json_text = response['Body'].read().decode('utf-8')
    return json.loads(json_text)

# get values from all the metadata files of the selected key
values = [read_metadata(bucket_name, file)[selected_key] for file in json_files]
# show al the values wiith the json  file names
st.write(values)