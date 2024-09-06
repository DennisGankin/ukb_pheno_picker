import argparse
import glob
import os
import pandas as pd
import subprocess
import dxpy

from distutils.version import LooseVersion

import pyspark
import re

def field_names_for_ids(field_id, data_dict_df):
    field_names = ["eid"]
    for _id in field_id:
        select_field_names = list(data_dict_df[data_dict_df.name.str.match(r'^p{}(_i\d+)?(_a\d+)?$'.format(_id))].name.values)
        field_names += select_field_names
    field_names = sorted([field for field in field_names], key=lambda n: LooseVersion(n))
        
    field_names = [f"participant.{f}" for f in field_names]
    return ",".join(field_names)

def main(datafields_filename):
    # Automatically discover dispensed dataset ID and load the dataset 
    dispensed_dataset_id = dxpy.find_one_data_object(typename='Dataset', name='app*.dataset', folder='/', name_mode='glob')['id']

    # Get project ID
    project_id = dxpy.find_one_project()["id"]
    dataset = (':').join([project_id, dispensed_dataset_id])

    # Execute command to extract dataset
    cmd = ["dx", "extract_dataset", dataset, "-ddd", "--delimiter", ","]
    subprocess.check_call(cmd)

    # Use the provided dataframe file name
    path = os.getcwd()
    data_dict_csv = glob.glob(os.path.join(path, "*.data_dictionary.csv"))[0]
    data_dict_df = pd.read_csv(data_dict_csv)
    print(data_dict_df.head())

    # load UKB field IDs you want to pull
    datafields = pd.read_csv(datafields_filename)
    # adding p to the id to match database field names
    datafields['field_id_prefix'] = 'p' + (datafields['field_id']).astype(str)
    field_ids = datafields['field_id']

    field_names = field_names_for_ids(field_ids, data_dict_df)

    #project = os.popen("dx env | grep project- | awk -F '\t' '{print $2}'").read().rstrip()
    #record = os.popen("dx describe *dataset | grep  record- | awk -F ' ' '{print $2}'").read().rstrip().split('\n')[0]
    #dataset = project + ":" + record

    cmd = ["dx", "extract_dataset", dataset, "--fields", field_names, "--delimiter", ",", "--output", "extracted_data.sql", "--sql"]
    subprocess.check_call(cmd)

    sc = pyspark.SparkContext()
    spark = pyspark.sql.SparkSession(sc)

    with open("extracted_data.sql", "r") as file:
        retrieve_sql=""
        for line in file: 
            retrieve_sql += line.strip()

    temp_df = spark.sql(retrieve_sql.strip(";"))
    pull_df = temp_df.toPandas()

    pull_df = pull_df.rename(columns=lambda x: re.sub('participant.','',x))
    pull_df_renamed = pull_df
    name_mapping = dict(zip(field_ids, datafields['field_name'])) 
    def map_name(x):
        xsplit = x.split('_')
        return name_mapping.get(xsplit[0], xsplit[0]) + ('_' + xsplit[1] if len(xsplit) > 1 else '')
    pull_df_renamed.columns = pull_df_renamed.columns.map(lambda x: map_name(x))

    print(pull_df_renamed)

    pull_df_renamed.to_csv('extracted_data.csv', index=False)            

if __name__ == "__main__":
    # Setting up argument parser
    parser = argparse.ArgumentParser(description='Process the dataframe file.')
    parser.add_argument('datafields_filename', type=str, help='The filename of the data fields CSV file')

    # Parse the argument
    args = parser.parse_args()

    # Call the main function with the provided filename
    main(args.datafields_filename)