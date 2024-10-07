import streamlit as st
import os
import boto3
import json
import pandas as pd
import ijson
from datetime import datetime, timezone

# AWS credentials from Streamlit secrets
aws_access_key_id = st.secrets["Access_key_ID"]
aws_default_region = st.secrets["AWS_DEFAULT_REGION"]

# Prompt user for AWS secret access key
aws_secret_access_key = st.text_input("Enter your AWS secret access key", type="password")
if aws_secret_access_key == "":
    st.stop()

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_default_region
)
bucket_name = 'dev-data-layer-datasets'

# Helper functions
def list_files_in_folder(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page['Contents']:
                files.append(obj['Key'])
    return files

def stream_json_file(s3, bucket, key, limit=1000):
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
    df = pd.DataFrame(properties_list)
    return df

def read_metadata(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    json_text = response['Body'].read().decode('utf-8')
    return json.loads(json_text)

def upload_json_to_s3(s3, data, bucket, key):
    # Convert the Python dictionary to a JSON string
    json_string = json.dumps(data, ensure_ascii=False, indent=4)
    # Upload the JSON string to S3
    s3.put_object(Body=json_string, Bucket=bucket, Key=key)

def main():
    st.title("Metadata Editor")
    

    # List JSON files in the 'datasets/' folder
    folder_prefix = 'datasets/'
    files = list_files_in_folder(bucket_name, folder_prefix)
    json_files = [file for file in files if file.endswith('json') and file.count('/') == 1]

    # Load input data file
    input_file = st.selectbox("Select a file", json_files)
    st.write(f"Selected file: {input_file}")
    name = input_file.split('/')[-1].split('.')[0] + ".json"

    if input_file:
        # Load input data
        input_data = stream_json_file(s3, bucket_name, input_file)
        dfData = convert_to_dataframe(input_data)
        
        show_preview = st.checkbox("Show data preview", value=True)
        if show_preview:
            st.subheader("Sample Data")
            st.table(dfData.head(5))
        all_columns = dfData.columns.tolist()

        # Remove geometry columns from suggestions
        for geom_col in ["geometry", "geom"]:
            if geom_col in all_columns:
                all_columns.remove(geom_col)

        # New section for pasting JSON metadata
        st.subheader("Paste Metadata JSON")
        pasted_metadata = st.text_area("Paste your metadata JSON here:", height=300)
        
        if pasted_metadata:
            try:
                metadata_file = json.loads(pasted_metadata)
                st.success("Metadata JSON successfully loaded!")
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON format: {e}")
                metadata_file = None
        else:
            # Existing code for loading metadata from S3
            try:
                files_metadata = list_files_in_folder(bucket_name, 'metadata/')
                metadata_file = None
                for f in files_metadata:
                    if name.split('.')[0].lower() in f.lower():
                        metadata_file = read_metadata(bucket_name, f)
                        break
            except Exception as e:
                st.error(f"Error loading metadata: {e}")
                metadata_file = None

        # Initialize metadata
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

        if metadata_file is not None:
            st.session_state.metadata = metadata_file
        else:
            st.session_state.metadata = metadataFormat.copy()

        # Remove extra keys not in metadataFormat
        st.session_state.metadata = {k: v for k, v in st.session_state.metadata.items() if k in metadataFormat}

        # Update 'updated_at' field
        st.session_state.metadata["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # **Handle duplicates in columns lists**
        columns_to_deduplicate = ["value_columns", "category_columns", "details_columns", "data_columns"]
        for col_list_name in columns_to_deduplicate:
            col_list = st.session_state.metadata.get(col_list_name, [])
            # Remove duplicates while preserving order
            seen = set()
            col_list_dedup = []
            for item in col_list:
                if item not in seen:
                    seen.add(item)
                    col_list_dedup.append(item)
            st.session_state.metadata[col_list_name] = col_list_dedup

        # General Information
        st.subheader("General Information")
        st.session_state.metadata["name"] = st.text_input("Name", st.session_state.metadata.get("name", ""))
        st.session_state.metadata["layer_id"] = st.text_input("Layer ID", st.session_state.metadata.get("layer_id", ""))
        st.session_state.metadata["geom_type"] = st.text_input("Geometry Type", st.session_state.metadata.get("geom_type", ""))
        st.session_state.metadata["geom_join"] = st.text_input("Geometry Join", st.session_state.metadata.get("geom_join", ""))
        st.session_state.metadata["description"] = st.text_area("Layer Description", st.session_state.metadata.get("description", ""))
        obj_details_options = ['id'] + all_columns
        st.session_state.metadata["obj_details_column"] = st.selectbox(
            "Object Details Column (e.g., Geoid)",
            obj_details_options,
            index=obj_details_options.index(st.session_state.metadata.get("obj_details_column", "id"))
        )

        st.session_state.metadata["has_biomass"] = st.checkbox("Has Biomass", st.session_state.metadata.get("has_biomass", False))
        st.session_state.metadata["has_county_geoid"] = st.checkbox("Has County Geoid", st.session_state.metadata.get("has_county_geoid", False))

        # Columns
        st.subheader("Columns")
        column_types = {
            "Value Columns": "value_columns",
            "Category Columns": "category_columns",
            "Details Columns": "details_columns",
            "Data Columns": "data_columns"
        }

        for column_type, metadata_key in column_types.items():
            selected_columns = st.multiselect(
                column_type,
                all_columns,
                st.session_state.metadata.get(metadata_key, [])
            )
            st.session_state.metadata[metadata_key] = list(dict.fromkeys(selected_columns))
            if selected_columns:
                st.table(dfData[selected_columns].head(5))

        # Get the keys from the columns dictionary
        column_options = [column['name'] for column in st.session_state.metadata["columns"]]

        # Check for mismatched columns
        if "data_columns" in st.session_state.metadata:
            mismatched_columns = [col for col in st.session_state.metadata["data_columns"] if col not in column_options]
            if mismatched_columns:
                st.error(f"The following columns are in default values but not in options: {mismatched_columns}")

        # Ensure default values are in options
        default_values = [col for col in st.session_state.metadata.get("data_columns", []) if col in column_options]

        st.session_state.metadata["data_columns"] = st.multiselect(
            "Select columns to include in the dataset:",
            options=column_options,
            default=default_values
        )

        st.session_state.metadata["tooltip-title"] = st.text_area(
            "Tooltip Title",
            st.session_state.metadata.get("tooltip-title", "")
        )
        st.session_state.metadata["tooltip-content"] = st.text_area(
            "Tooltip Content (e.g., {{Geoid}})",
            st.session_state.metadata.get("tooltip-content", "")
        )

        # Other Information
        st.subheader("Other Information")
        st.session_state.metadata["s3_file_path"] = st.text_input(
            "S3 File Path",
            st.session_state.metadata.get("s3_file_path", "")
        )
        st.session_state.metadata["view_name"] = st.text_input(
            "View Name",
            st.session_state.metadata.get("view_name", "")
        )
        st.session_state.metadata["human_identifier_field"] = st.text_input(
            "Human Identifier",
            st.session_state.metadata.get("human_identifier_field", "")
        )
        st.session_state.metadata["mandatory_filter"] = st.multiselect(
            "Mandatory Filter",
            all_columns,
            st.session_state.metadata.get("mandatory_filter", [])
        )
        st.text("Layer Access Level: 0 - Free Users, 1 - Freemium Users, 2 - Premium Users")
        try:
            st.session_state.metadata["layer_access_level"] = int(st.text_input(
                "Layer Access Level",
                st.session_state.metadata.get("layer_access_level", 2)
            ))
        except ValueError:
            st.error("Please enter a valid integer for Layer Access Level.")
        st.session_state.metadata["supplier_layer"] = st.checkbox(
            "Supplier Layer",
            value=st.session_state.metadata.get("supplier_layer", False)
        )

        # Columns Details
        st.subheader("Columns Details")
        selected_columns = set(
            st.session_state.metadata["value_columns"] +
            st.session_state.metadata["category_columns"] +
            st.session_state.metadata["details_columns"] +
            st.session_state.metadata["data_columns"]
        ) - {"geom"}

        columns = []
        for column_name in selected_columns:
            column_data = next(
                (c for c in st.session_state.metadata.get("columns", []) if c["name"] == column_name),
                None
            )
            if column_data is None:
                column_data = {
                    "name": column_name,
                    "label": column_name,
                    "type": "text",
                    "description": ""
                }

            st.write(f"**Column:** {column_name}")
            column_data["label"] = st.text_input(
                "Label",
                column_data.get("label", column_name),
                key=f"label_{column_name}"
            )
            data_types = ["text", "float", "int", "boolean"]
            column_data["type"] = st.selectbox(
                "Type",
                data_types,
                index=data_types.index(column_data.get("type", "text")) if column_data.get("type", "text") in data_types else 0,
                key=f"type_{column_name}"
            )
            column_data["description"] = st.text_area(
                "Description",
                column_data.get("description", ""),
                key=f"description_{column_name}"
            )
            columns.append(column_data)

        st.session_state.metadata["columns"] = columns

        # Visualization Settings
        st.subheader("Visualization Settings")

        # Get existing visualization settings as JSON string
        visualization_json = json.dumps(
            st.session_state.metadata.get("visualization", {}),
            ensure_ascii=False,
            indent=4
        )

        # Display a text area for the visualization JSON
        visualization_input = st.text_area(
            "Visualization Settings (JSON format)",
            visualization_json,
            height=500
        )

        # Try to parse the JSON input
        try:
            visualization_data = json.loads(visualization_input)
            st.session_state.metadata["visualization"] = visualization_data
            visualization_error = None
        except json.JSONDecodeError as e:
            visualization_error = str(e)
            st.error(f"Invalid JSON format: {visualization_error}")

        # Display Updated Metadata
        st.subheader("Updated Metadata")
        st.json(st.session_state.metadata)

        # Save Metadata
        if st.button("Save Metadata"):
            if visualization_error:
                st.error("Cannot save metadata due to invalid visualization JSON.")
            else:
                # Ensure 'geom' is included in data_columns
                if 'geom' not in st.session_state.metadata["data_columns"]:
                    st.session_state.metadata["data_columns"].append('geom')
                upload_json_to_s3(
                    s3,
                    st.session_state.metadata,
                    bucket_name,
                    'metadata/' + name.split('.')[0] + "_metadata.json"
                )
                st.success("Metadata saved successfully!")

    else:
        st.warning("Please select an input data file to get started.")


if __name__ == "__main__":
    main()







