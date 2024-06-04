import streamlit as st
import os
import boto3
import json
import pandas as pd
import ijson
from datetime import datetime, timezone
from io import BytesIO
import pickle

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

object_key = 'dashboard/dict.pkl'
# Create a buffer
pickle_buffer = BytesIO()
# Download the object 'your-pickle-file.pkl' from the bucket 'your-bucket-name'
s3.download_fileobj(bucket_name, object_key, pickle_buffer)
# Set the buffer's position to the start
pickle_buffer.seek(0)
# Load the data from the buffer
data_dict = pickle.load(pickle_buffer)
# with open('dict.pkl', 'rb') as file:
#     data_dict = pickle.load(file)

# Get the list of unique state names from one of the dataframes
state_names = data_dict[list(data_dict.keys())[0]]['state_name'].unique()

# Create a Streamlit selectbox for state selection
selected_state = st.selectbox('Select a state', state_names)

# Iterate over the dataframes in the dictionary
for key, df in data_dict.items():
    # Apply the state filter to each dataframe
    filtered_df = df[df['state_name'] == selected_state]
    
    # Check if the filtered dataframe has more than 1 row
    if len(filtered_df) > 1:
        # Calculate the sum of float columns
        float_cols = filtered_df.select_dtypes(include=['float','int']).columns
        total_row = filtered_df[float_cols].sum().tolist()
        
        # Create a dictionary for the total row
        total_row_dict = dict(zip(filtered_df.columns, ['-'] * len(filtered_df.columns)))
        total_row_dict.update(dict(zip(float_cols, total_row)))
        
        # Convert the total row dictionary to a DataFrame
        total_row_df = pd.DataFrame(total_row_dict, index=['Total'])
        
        # Concatenate the filtered dataframe with the total row DataFrame
        filtered_df = pd.concat([filtered_df, total_row_df])
    
    # Display the filtered dataframe
    st.subheader(f"Dataframe: {key}")
    st.dataframe(filtered_df)