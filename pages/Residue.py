import streamlit as st
import pandas as pd

# st.set_page_config(
#     page_title="Residue",
#     page_icon="👋",
# )

dfResidue = pd.read_csv("biomassData.csv")
dfResidue.columns = [col.title().replace('_', ' ') for col in dfResidue.columns]
dfResidue['Biomas Tons'] /= 1000  # Convert Biomass Tons to Thousands

# Sidebar filtering functions
def create_sidebar_filters(df):
    source_options = df['Source'].unique()
    state_options = df['State'].unique()

    source_selection = st.sidebar.selectbox(
        "Select Detailed Source:",
        source_options
    )

    state_selection = st.sidebar.selectbox(
        "Select State:",
        state_options
    )

    return source_selection, state_selection

def filter_by_state(df, state_selection):
    sector_options = df['Biomass Sector'].unique()
    commodity_options = df['Biomass Commodity'].unique()
    type_options = df['Biomass Type'].unique()

    sector_selections = st.sidebar.multiselect("Biomass Sector", sector_options, default=sector_options)
    commodity_selections = st.sidebar.multiselect("Biomass Commodity", commodity_options, default=commodity_options)
    type_selections = st.sidebar.multiselect("Biomass Type", type_options, default=type_options)

    df_filtered = df[
        df['Biomass Sector'].isin(sector_selections) &
        df['Biomass Commodity'].isin(commodity_selections) &
        df['Biomass Type'].isin(type_selections)
    ]

    return df_filtered

source_selection, state_selection = create_sidebar_filters(dfResidue)

df_filtered = dfResidue[
    dfResidue['Source'].isin([source_selection]) &
    dfResidue['State'].isin([state_selection])
]

df_filtered = filter_by_state(df_filtered, state_selection)

# Sum and display total biomass in million tons
total_biomass_million_tons = df_filtered['Biomas Tons'].sum() / 1000
st.write(f"Total Biomass: {total_biomass_million_tons:.3f} Million Tons")

# Main page adjustments for Tons filter and group by functionality
county_biomass = df_filtered.groupby('County')['Biomas Tons'].sum().reset_index()

min_tons = st.number_input(
    "Enter Minimum Biomas Tons:",
    min_value=0,
    value=0,
    step=1
)

df_filtered = df_filtered[df_filtered['County'].isin(county_biomass[county_biomass['Biomas Tons'] >= min_tons]['County'])]
st.write(f"Total Counties Satisfying Filter: {df_filtered['County'].nunique()}")

group_by_columns = st.multiselect(
    "Select Columns to Group By:",
    options=df_filtered.columns.tolist(),
    default=['State']
)

if group_by_columns:
    df_grouped = df_filtered.groupby(group_by_columns).agg({'Biomas Tons': 'sum'}).reset_index()
    df_grouped = df_grouped.sort_values(by='Biomas Tons', ascending=False)
    st.write("Grouped by Selected Columns:", df_grouped)

total_biomass = df_filtered['Biomas Tons'].sum()
dfCounty = df_filtered.groupby('County')['Biomas Tons'].sum().reset_index().sort_values(by='Biomas Tons', ascending=False)
dfCounty['Percent of State Total'] = dfCounty['Biomas Tons'] / total_biomass * 100
st.write("Top 5 Counties by Biomass (in thousands of tons, percent of state total):", dfCounty.head(5))
st.caption("Note: Percentages are calculated against the total biomass of the selected state, not just the top 5 or filtered counties.")