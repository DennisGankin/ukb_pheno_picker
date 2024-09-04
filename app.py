import streamlit as st
import pandas as pd

# Read the TSV file and extract 'field_id' and 'title' columns
try:
    # Assuming the file is in the same directory
    df_fields = pd.read_csv('data/fields.tsv', sep='\t', usecols=['field_id', 'title'])
except FileNotFoundError:
    st.error("The file 'fields.tsv' was not found. Please upload it.")

# Combine 'title' and 'field_id' to make each option unique
df_fields['combined'] = df_fields['title'] + ' (' + df_fields['field_id'].astype(str) + ')'

# Create options for the multi-select widget
options = df_fields['combined'].tolist()

st.title("UK Biobank Fields")
st.write("Select one or more fields to create a table with field names and IDs. You can download the table as a csv file.")

# Allow the user to select multiple values from the combined list
selected_combined = st.multiselect("Select titles (with field IDs)", options)

# Automatically create and display the DataFrame when selected options change
if selected_combined:
    # Extract the selected rows based on combined values
    selected_rows = df_fields[df_fields['combined'].isin(selected_combined)]
    
    # Create a new DataFrame with 'Selected Titles', 'Field IDs', and 'Field Name'
    df_selected = pd.DataFrame({
        'field_title': selected_rows['title'],
        'field_id': selected_rows['field_id'],
        'field_name': selected_rows['title'].str.replace(" ","_").str.lower()  # Populate 'Field Name' with titles initially
    })
    
    # Display the DataFrame with editable 'Field Name' column
    st.write("Edit the data if needed.")
    df_selected = st.data_editor(df_selected, num_rows="dynamic", key="data_editor")
    
    st.write("Updated DataFrame:")
    st.dataframe(df_selected)
    
else:
    st.write("No titles selected yet.")

# Keep the download button for CSV
if selected_combined:
    st.download_button(
        label="Download CSV",
        data=df_selected.to_csv(index=False).encode('utf-8'),
        file_name='ukb_pheno_fields.csv',
        mime='text/csv'
    )