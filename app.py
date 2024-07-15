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

folder_prefix = 'datasets/'
files = list_files_in_folder(bucket_name, folder_prefix)
json_files = [file for file in files if file.endswith('json') and file.count('/') == 1]


def stream_json_file(s3,bucket, key, limit=1000):
    response = s3.get_object(Bucket=bucket, Key=key)
    
    objects = ijson.items(response['Body'], 'features.item')
    
    # Collect up to `limit` features
    limited_features = [feature for _, feature in zip(range(limit), objects)]
    
    # Reconstruct a partial JSON object
    partial_json = {
        'type': 'FeatureCollection',
        'features': limited_features
    }
    
    
    return partial_json

def convert_to_dataframe(geojson_features):
    # Extract the properties from each feature
    properties_list = [feature['properties'] for feature in geojson_features['features']]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(properties_list)
    
    return df

def read_metadata(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    # Assuming the file is UTF-8 encoded, adjust the encoding as needed
    json_text = response['Body'].read().decode('utf-8')
    return json.loads(json_text)

def upload_json_to_s3(s3,data, bucket, key):    
    # Convert the Python dictionary to a JSON string
    json_string = json.dumps(data, ensure_ascii=False, indent=4)
    
    # Upload the JSON string to S3
    s3.put_object(Body=json_string, Bucket=bucket, Key=key)

def main():
    st.title("Metadata Editor")

    # Load input data file
    input_file = st.selectbox("Select a file", json_files)
    # get the name of the file
    # name = input_file.name
    st.write(input_file)
    name = input_file.split('/')[-1].split('.')[0]+".json"

    if input_file is not None:
        # Load existing metadata if available
        # metadata_file = r"C:\Users\fasiu\OneDrive\Documents\GitHub\data-layer\Datasets\Processed\Final\\"+name.split('.')[0]+"_metadata.json"
        
        # Get input data columns
        input_data = stream_json_file(s3,bucket_name, input_file)
        input_data = convert_to_dataframe(input_data)
        st.table(input_data.head(5))
        all_columns = input_data.columns.tolist()

        check = 0
        try:
            files_metadata = list_files_in_folder(bucket_name, 'metadata/')
            
            for f in files_metadata:
                if name.split('.')[0].lower() in f.lower():
                    metadata_file = read_metadata(bucket_name, f)
                    check = 1
                    break
            if check == 0:
                # Adding exception to handle the case where the metadata file is not found
                asdfasdfdsdf

        except:
        #     file = st.selectbox("Select a metadata file", list_files_in_folder(bucket_name, 'metadata/'))
        #     metadata_file = read_metadata(bucket_name, file)
        #     input_columns = set(all_columns)
        #     metadata_file["has_biomass"] = False
        #     metadata_file["has_county_geoid"] = False
        #     metadata_file["value_columns"] = [col for col in metadata_file["value_columns"] if col in input_columns]
        #     metadata_file["category_columns"] = [col for col in metadata_file["category_columns"] if col in input_columns]
        #     metadata_file["details_columns"] = [col for col in metadata_file["details_columns"] if col in input_columns]
        #     metadata_file["data_columns"] = [col for col in metadata_file["data_columns"] if col in input_columns]
        #     metadata_file["columns"] = [col for col in metadata_file["columns"] if col in input_columns]
            metadata_file = None


        if metadata_file is not None:
            st.session_state.metadata = metadata_file
        else:
            st.session_state.metadata = {
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
                "tooltip-title": [],
                "tooltip-content": "",
                "s3_file_path": "",
                "view_name": "",
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") ,
                "detials_modals": [],
                "columns": [],
                "calculated_fields": [],
                "human_identifier_field": "",
                "mandatory_filter": [],
                "layer_access_level":2,
                "supplier_layer": ""
            }


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
                "tooltip-title": [],
                "tooltip-content": "",
                "s3_file_path": "",
                "view_name": "",
                "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") ,
                "detials_modals": [],
                "columns": [],
                "calculated_fields": [],
                "human_identifier_field": "",
                "mandatory_filter": [],
                'layer_access_level':2,
                "supplier_layer": ""
            }
        
        # Remove all keys in metadata that are not in metadataFormat
        st.session_state.metadata = {k: v for k, v in st.session_state.metadata.items() if k in metadataFormat}

        # Remove geometry column from suggestions
        if "geometry" in all_columns:
            all_columns.remove("geometry")

        if "geom" in all_columns:
            all_columns.remove("geom")  

        # st.session_state.metadata["data_columns"] remove geom from data columns
        st.session_state.metadata["data_columns"] = [col for col in st.session_state.metadata["data_columns"] if col != "geom"]
            
        # Render input fields
        st.subheader("General Information")
        st.session_state.metadata["name"] = st.text_input("Name", st.session_state.metadata["name"])
        st.session_state.metadata["layer_id"] = st.text_input("Layer Id", st.session_state.metadata["layer_id"])
        st.session_state.metadata["geom_type"] = st.text_input("Geometry Type", st.session_state.metadata["geom_type"])
        st.session_state.metadata["geom_join"] = st.text_input("Geometry Join", st.session_state.metadata["geom_join"])
        st.session_state.metadata["description"] = st.text_area("Layer description", st.session_state.metadata["description"])
        st.session_state.metadata["obj_details_column"] = st.selectbox("obj_details_column (eg, Geoid)", ['id']+all_columns)

        st.session_state.metadata["has_biomass"] = st.checkbox("Has Biomass", st.session_state.metadata["has_biomass"])
        
        try:
            st.session_state.metadata["has_county_geoid"] = st.checkbox("Has County Geoid", st.session_state.metadata["has_county_geoid"])
        except:
            st.session_state.metadata["has_county_geoid"] = st.checkbox("Has County Geoid")

        st.subheader("Columns")

        # if county geoid is present then add state_name to all columns
        if st.session_state.metadata["has_county_geoid"]:
            all_columns.append("state_name")

        dfData = pd.DataFrame(input_data)
        st.session_state.metadata["value_columns"] = st.multiselect("Value Columns", all_columns, st.session_state.metadata["value_columns"])
        st.table(dfData[st.session_state.metadata["value_columns"]].head(5))
        st.session_state.metadata["category_columns"] = st.multiselect("Category Columns", all_columns, st.session_state.metadata["category_columns"])
        st.table(dfData[st.session_state.metadata["category_columns"]].head(5))
        st.text(st.session_state.metadata["details_columns"])
        st.text(all_columns)
        st.text(st.session_state.metadata["details_columns"])
        st.session_state.metadata["details_columns"] = st.multiselect("Details Columns", all_columns, st.session_state.metadata["details_columns"])
        # st.table(dfData[st.session_state.metadata["details_columns"]].head(5))
        st.session_state.metadata["data_columns"] = st.multiselect("Data Columns", all_columns, st.session_state.metadata["data_columns"])
        # st.table(dfData[st.session_state.metadata["data_columns"]].head(5))
        
        try:
            st.session_state.metadata["tooltip-title"] = st.text_area("tooltip-title", st.session_state.metadata["tooltip-title"])

        except:
            st.session_state.metadata["tooltip-title"] = st.text_area("tooltip-title")

        try:
            st.session_state.metadata["tooltip-content"] = st.text_area("tooltip-content (eg, {{Geoid}})", st.session_state.metadata["tooltip-content"])
        except:
            st.session_state.metadata["tooltip-content"] = st.text_area("tooltip-content (eg, {{Geoid}})")
        # st.table(dfData[st.session_state.metadata["tooltip-title"]].head(5))
        st.subheader("Other Information")
        st.session_state.metadata["s3_file_path"] = st.text_input("S3 File Path", st.session_state.metadata["s3_file_path"])
        st.session_state.metadata["view_name"] = st.text_input("View Name", st.session_state.metadata["view_name"])
        # add time in utc
        st.session_state.metadata["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") 

        try:
            st.session_state.metadata["human_identifier_field"] = st.text_input("Human Identifier", st.session_state.metadata["human_identifier_field"])
        except:
            st.session_state.metadata["human_identifier_field"] = st.text_input("Human Identifier")

        try:
            st.session_state.metadata["mandatory_filter"] = st.multiselect("Mandatory Filter", all_columns, st.session_state.metadata["mandatory_filter"])
        except:
            st.session_state.metadata["mandatory_filter"] = st.multiselect("Mandatory Filter", all_columns)

        # add a new field called layer_access_level input must be a number
        # if the input is not a number then it should be empty
        # if its value is 0 it will be accessable to free users and all others.
        # if its value is 1 it will be accessable to freemium users and premium users.
        # it its value is 2 it will be accessable to premium users only.
        st.text("Layer Access Level 0: Free Users, 1: Freemium Users, 2: Premium Users")
        try:
            st.session_state.metadata["layer_access_level"] = int(st.text_input("Layer Access Level", st.session_state.metadata["layer_access_level"]))
        except:
            st.session_state.metadata["layer_access_level"] = int(st.text_input("Layer Access Level"),2)

        # add a new field called supplier_layer input can be true or empty
        # if the input is true then it will be a supplier layer
        try:
            st.session_state.metadata["supplier_layer"] = st.checkbox("Supplier Layer", value=st.session_state.metadata["supplier_layer"])
        except:
            st.session_state.metadata["supplier_layer"] = st.checkbox("Supplier Layer")

        # Render columns
        st.subheader("Columns Details")
        selected_columns = set(
            st.session_state.metadata["value_columns"] +
            st.session_state.metadata["category_columns"] +
            st.session_state.metadata["details_columns"] +
            st.session_state.metadata["data_columns"] 
        )
    
        # geom should never be included in columns
        selected_columns = list(selected_columns - {"geom"})
        
        columns = []
        for column_name in selected_columns:
            column_data = next((c for c in st.session_state.metadata["columns"] if c["name"] == column_name), None)
            if column_data is None:
                column_data = {"name": column_name, "label": column_name, "type": "text", "description": ""}

            st.write(f"Column: {column_name}")
            column_data["label"] = st.text_input("Label", column_data["label"])

            list_ = ["text", "float", "int", "boolean"]
            # index = list_.index(column_data["type"])
            column_data["type"] = st.selectbox("Type", ("text", "float", "int", "boolean"), list_.index(column_data["type"]),key= column_name)
            column_data["description"] = st.text_area("Description", column_data["description"],key= column_name + "2")
            columns.append(column_data)

        st.session_state.metadata["columns"] = columns
        
        st.json(st.session_state.metadata)

        st.session_state.metadata["data_columns"] = st.session_state.metadata["data_columns"] + ['geom']
        # Save metadata
        if st.button("Save Metadata"):
            upload_json_to_s3(s3,st.session_state.metadata, bucket_name, 'metadata/' + name.split('.')[0]+"_metadata.json")
    else:
        st.warning("Please upload an input data file to get started.")

if __name__ == "__main__":
    main()