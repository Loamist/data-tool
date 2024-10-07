import streamlit as st
import geopandas as gpd
import folium
from folium.plugins import HeatMap, MarkerCluster
from streamlit_folium import folium_static
import matplotlib.pyplot as plt
import json

# Function to display error message for incorrect column selection
def display_error(message):
    st.error(message)

# Function to read the uploaded file and convert to GeoDataFrame
def load_geospatial_data(file):
    try:
        # Attempt to read as a GeoJSON
        geo_df = gpd.read_file(file)
        return geo_df
    except Exception:
        # Attempt to read as a generic JSON and convert to GeoDataFrame
        try:
            data = json.load(file)
            geo_df = gpd.GeoDataFrame.from_features(data['features'])
            return geo_df
        except Exception as e:
            display_error(f"Error loading file: {e}")
            return None

# Function to render map using Folium
def render_map(geo_df, symbology, column=None, graduated_style=None):
    # Initialize Folium map centered on the average coordinates of the dataset
    m = folium.Map(location=[geo_df.geometry.centroid.y.mean(), geo_df.geometry.centroid.x.mean()], zoom_start=10)

    if symbology == "Flat":
        # Simple flat symbology: color all points uniformly
        for _, row in geo_df.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=5,
                color='blue',
                fill=True,
                fill_color='blue',
                fill_opacity=0.7,
            ).add_to(m)
        st.write("Displaying flat symbology.")

    elif symbology == "Categorized":
        # Categorical symbology with unique colors for each category
        if geo_df[column].dtype == 'object':
            unique_values = geo_df[column].unique()
            colormap = plt.cm.get_cmap('Set1', len(unique_values))
            for _, row in geo_df.iterrows():
                color = colormap(unique_values.tolist().index(row[column]))
                folium.CircleMarker(
                    location=[row.geometry.y, row.geometry.x],
                    radius=5,
                    color=f'#{int(color[0]*255):02x}{int(color[1]*255):02x}{int(color[2]*255):02x}',
                    fill=True,
                    fill_color=f'#{int(color[0]*255):02x}{int(color[1]*255):02x}{int(color[2]*255):02x}',
                    fill_opacity=0.7,
                ).add_to(m)
            st.write("Displaying categorized symbology.")
        else:
            display_error("Selected column is not suitable for categorization. Please select a categorical column.")

    elif symbology == "Graduated":
        # Graduated symbology based on numerical data with additional style choices
        try:
            geo_df[column] = geo_df[column].astype(float)
        except:
            pass
        if geo_df[column].dtype in ['int64', 'float64']:
            graduated_style = st.sidebar.selectbox("Select Graduated Style", ["By Size", "By Color", "By Both"])
            min_val, max_val = geo_df[column].min(), geo_df[column].max()
            
            for _, row in geo_df.iterrows():
                value = row[column]
                normalized_value = (value - min_val) / (max_val - min_val)
                # Gradient color from green to red
                color = plt.cm.get_cmap('RdYlGn_r')(normalized_value)  # Reversed to go from green to red
                
                # Adjust radius based on value
                radius = 5 + normalized_value * 10
                
                # Determine style based on the selected option
                if graduated_style == "By Size":
                    folium.CircleMarker(
                        location=[row.geometry.y, row.geometry.x],
                        radius=radius,
                        color='blue',
                        fill=True,
                        fill_color='blue',
                        fill_opacity=0.7,
                    ).add_to(m)
                elif graduated_style == "By Color":
                    folium.CircleMarker(
                        location=[row.geometry.y, row.geometry.x],
                        radius=5,
                        color=f'#{int(color[0]*255):02x}{int(color[1]*255):02x}{int(color[2]*255):02x}',
                        fill=True,
                        fill_color=f'#{int(color[0]*255):02x}{int(color[1]*255):02x}{int(color[2]*255):02x}',
                        fill_opacity=0.7,
                    ).add_to(m)
                elif graduated_style == "By Both":
                    folium.CircleMarker(
                        location=[row.geometry.y, row.geometry.x],
                        radius=radius,
                        color=f'#{int(color[0]*255):02x}{int(color[1]*255):02x}{int(color[2]*255):02x}',
                        fill=True,
                        fill_color=f'#{int(color[0]*255):02x}{int(color[1]*255):02x}{int(color[2]*255):02x}',
                        fill_opacity=0.7,
                    ).add_to(m)
            st.write(f"Displaying graduated symbology by {graduated_style.lower()}.")
        else:
            display_error("Selected column is not suitable for graduated symbology. Please select a numerical column.")

    elif symbology == "Point Cluster":
        # Cluster symbology for point data
        marker_cluster = MarkerCluster().add_to(m)
        for _, row in geo_df.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=5,
                color='blue',
                fill=True,
                fill_color='blue',
                fill_opacity=0.7,
            ).add_to(marker_cluster)
        st.write("Displaying point cluster symbology.")

    elif symbology == "Heat Map":
        # Heatmap symbology
        if geo_df[column].dtype in ['int64', 'float64']:
            heat_data = [[row.geometry.y, row.geometry.x, row[column]] for _, row in geo_df.iterrows()]
            HeatMap(heat_data).add_to(m)
            st.write("Displaying heat map symbology.")
        else:
            display_error("Selected column is not suitable for heat map. Please select a numerical column.")

    # Display map
    folium_static(m)

# Streamlit App Layout
st.title("Symbology Demo with GeoPandas and Streamlit")
st.sidebar.title("Upload and Select Options")

# File Upload Section
uploaded_file = st.sidebar.file_uploader("Upload a GeoJSON or JSON file", type=["geojson", "json"])
if uploaded_file:
    geo_df = load_geospatial_data(uploaded_file)
    
    if geo_df is not None:
        st.sidebar.success("File uploaded successfully!")
        
        # Display the dataframe
        st.write("Uploaded GeoDataFrame:")
        st.write(geo_df.head())

        # Symbology Options
        symbology = st.sidebar.selectbox("Select Symbology Type", ["Flat", "Categorized", "Graduated", "Point Cluster", "Heat Map"])
        
        # Column Selection Based on Symbology Type
        column = None
        graduated_style = None
        if symbology in ["Categorized", "Graduated", "Heat Map"]:
            column = st.sidebar.selectbox("Select Column for Symbology", geo_df.columns)
        
        # Render Map Based on Selection
        render_map(geo_df, symbology, column, graduated_style)
else:
    st.info("Please upload a GeoJSON or JSON file to proceed.")
