from dotenv import load_dotenv
load_dotenv()
"""
Use This Script to 
    1. extrtact the realtime news
    2. scrape the urls
    3. preprocess the scraped data
    4. load the preprocessed data to GCP BigQuery
"""
from data_extractor import collect_news
from preprocessor import extract_and_preprocess,combine_data,create_table_from_csv_direct
import pandas as pd 

def extract_data_upload_bq():
    print(collect_news())
    print(extract_and_preprocess())
    print(combine_data())
    print(create_table_from_csv_direct('./data/news.csv'))
