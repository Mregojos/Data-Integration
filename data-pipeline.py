# data-pipeline.py
#--------------------Import Libraries----------------------------------#
from prefect import task, flow
import urllib.request
import pandas as pd
import os

#--------------------Variables-----------------------------------------#
url_data = ['https://open.canada.ca/data/dataset/b0e112e9-cf53-4e79-8838-23cd98debe5b/resource/9c47c662-d856-445f-b0db-72a1a7e5b805/download/20222024_outlook_n21_en_221114.xlsx', 
            'https://open.canada.ca/data/dataset/b0e112e9-cf53-4e79-8838-23cd98debe5b/resource/4ea0ec40-a31b-443c-be95-fe114e780821/download/20222024_outlook_n16_en_221114.xlsx',
            'https://open.canada.ca/data/dataset/ab36a4ab-9b69-49b1-8be6-6faa3f0d0c67/resource/c546ea24-b0ee-481f-8afe-96a0d5742519/download/cfppsc_coop02-2023-04-03.csv']

filenames = ['20222024_outlook_n21_en_221114.xlsx', 
             '20222024_outlook_n16_en_221114.xlsx',
             'cfppsc_coop02-2023-04-03.csv']

folder = 'original-data/'

#--------------------Extract: Download files----------------------------#
@task(log_prints=True, name='Download Files')
def download_files():
    for url, filename in zip(url_data, filenames):
        urllib.request.urlretrieve(url, f"{folder}{filename}")
        
#--------------------Transform: Data Processing--------------------------#
@task(log_prints=True, name='Convert xlsx to csv')
def xlsx_to_csv():
    for file in os.listdir("original-data"):
        if (file[-5:-1] + file[-1]) == ".xlsx":
            data = pd.read_excel("original-data/" + file, index_col=1)
            data.to_csv("processed-data/" + file.rstrip(".xlsx") + ".csv")
        elif (file[-4:-1] + file[-1]) == ".csv":
            data = pd.read_csv("original-data/" + file, index_col=1)
            data.to_csv("processed-data/" + file)

@task(log_prints=True, name='Create DataFrames')
def dataframe():
    outlook_noc_2021 = pd.read_csv("processed-data/20222024_outlook_n21_en_221114.csv")
    outlook_noc_2016 = pd.read_csv("processed-data/20222024_outlook_n16_en_221114.csv")
    institutions_programs = pd.read_csv("processed-data/cfppsc_coop02-2023-04-03.csv")
    return outlook_noc_2021, outlook_noc_2016, institutions_programs

@task(log_prints=True, name='Data Cleaning')
def data_cleaning(institutions_programs):
    # Data cleaning: institutions_programs table
    # Drop the contact column
    institutions_programs = institutions_programs.drop(["contact"], axis=1)

    # Create Dataframe (English and French)
    f_col = []
    e_col = []
    for column in institutions_programs.columns:
        if column[-1] != 'e':
            f_col.append(column)
        else:
            e_col.append(column)

    institutions_programs_e = institutions_programs.drop(f_col, axis=1)
    institutions_programs_f = institutions_programs.drop(e_col, axis=1)

    # Remove _e in column names
    institutions_programs_e.columns = institutions_programs_e.columns.str.rstrip('_e')

    # Rename the columns
    institutions_programs_cleaned = institutions_programs_e.rename(columns={"institution_nam":"institution_name", "program_typ":"program_type"})

    # Drop Duplicates
    institutions_programs_cleaned = institutions_programs_cleaned.drop_duplicates()
    
    return institutions_programs_cleaned

#--------------------Load: Local Storage (in csv)------------------------------#
@task(log_prints=True, name='Load to local storage')
def load(outlook_noc_2021, outlook_noc_2016, institutions_programs_cleaned):
    outlook_noc_2021.to_csv('processed-data/outlook_noc_2021.csv')
    outlook_noc_2016.to_csv('processed-data/outlook_noc_2016.csv')
    institutions_programs_cleaned.to_csv('processed-data/institutions_programs.csv')
    
#--------------------Workflow----------------------------------------#
@flow(log_prints=True, name='Prefect-Workflow')
def workflow():
    download_files()
    xlsx_to_csv()
    outlook_noc_2021, outlook_noc_2016, institutions_programs = dataframe()
    institutions_programs_cleaned = data_cleaning(institutions_programs)
    load(outlook_noc_2021, outlook_noc_2016, institutions_programs_cleaned)

#--------------------Execution----------------------------------------#
if __name__=="__main__":
    workflow()
    
# python ./data-pipeline.py
