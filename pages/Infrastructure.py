import streamlit as st
import pickle
import os
import pandas as pd


with open('dict.pkl', 'rb') as file:
    data_dict = pickle.load(file)

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