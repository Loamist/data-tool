import streamlit as st
import boto3
import json
from datetime import datetime, timezone

aws_access_key_id = st.secrets["Access_key_ID"]
aws_default_region = st.secrets["AWS_DEFAULT_REGION"]

aws_secret_access_key = st.text_input("Enter your AWS secret access key", type="password")
if aws_secret_access_key == "":
    st.stop()

s3 = boto3.client(
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

def read_metadata(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    json_text = response['Body'].read().decode('utf-8')
    return json.loads(json_text)

metadataFormat = {
    "name": "",
    "layer_id": "",
    "geom_type": "",
    "geom_join": "",
    "description": "",
    "obj_details_column": "",
    "has_biomass": False,
    "has_county_geoid": False,
    "value_columns": [],
    "category_columns": [],
    "details_columns": [],
    "data_columns": [],
    "tooltip-title": "",
    "tooltip-content": "",
    "s3_file_path": "",
    "view_name": "",
    "updated_at": "",
    "details_modals": [],
    "columns": [],
    "calculated_fields": [],
    "human_identifier_field": "",
    "mandatory_filter": [],
    "layer_access_level": 2,
    "supplier_layer": False,
    "visualization": {}
}

def safe_get(dictionary, key, default=None):
    return dictionary.get(key, default)

folder_prefix = 'metadata/'
files = list_files_in_folder(bucket_name, folder_prefix)
json_files = [file for file in files if file.endswith('json') and file.count('/') == 1]

keys = list(metadataFormat.keys())

selected_key = st.selectbox('Select a key', keys)

values = {}
for file in json_files:
    metadata = read_metadata(bucket_name, file)
    values[file] = safe_get(metadata, selected_key, "N/A")

st.subheader(f"Values for '{selected_key}'")
for key, value in values.items():
    st.write(f"**{key}**")
    st.write(value)
    st.write('---')
