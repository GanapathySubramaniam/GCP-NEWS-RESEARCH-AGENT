import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

def create_dataset_if_not_exists(client, project_id, dataset_id, location="US"):
    """Create a BigQuery dataset if it doesn't exist."""
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        # Create the dataset
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location  # e.g., "US", "EU", "asia-northeast1"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id} in location {location}")

def create_table_from_csv_direct(file):
    client = bigquery.Client()
    
    project_id = os.environ['PROJECT_ID']  
    dataset_id = "news"     
    table_id = "news_data" 
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(client, project_id, dataset_id)
    
    # Schema definition
    schema = [
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("headline", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("extracted_text", "STRING"),
        bigquery.SchemaField("sentiment", "STRING"),
        bigquery.SchemaField("entities", "STRING"),
        bigquery.SchemaField("detailed_category", "STRING"),
    ]
    
    # Job configuration
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,  # Important for CSV with multiline text
        allow_jagged_rows=False,
        max_bad_records=0,
    )
    
    try:
        # Load data from CSV file
        with open(file, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
        job.result()  # Wait for completion
        print(f"Loaded {job.output_rows} rows into {table_ref}")
        
        # Verify the load
        table = client.get_table(table_ref)
        print(f"Table {table_ref} now has {table.num_rows} rows and {len(table.schema)} columns")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        # Print job errors if available
        if 'job' in locals():
            for error in job.errors or []:
                print(f"Job error: {error}")

# Alternative: More robust version with better error handling
def create_news_table_and_insert_data_robust(file):
    """More robust version with comprehensive error handling."""
    client = bigquery.Client()
    
    project_id = os.environ['PROJECT_ID']
    dataset_id = "news"
    table_id = "news_data"
    
    try:
        # Step 1: Create dataset if it doesn't exist
        dataset_ref = f"{project_id}.{dataset_id}"
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset {dataset_id} exists.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset.description = "Dataset for news articles data"
            dataset = client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {dataset_id}")
        
        # Step 2: Define table schema
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        schema = [
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("headline", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extracted_text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sentiment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("entities", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("detailed_category", "STRING", mode="NULLABLE"),
        ]
        
        # Step 3: Create or replace table
        table = bigquery.Table(table_ref, schema=schema)
        table.description = "News articles with sentiment analysis and entity extraction"
        
        try:
            # Try to delete existing table first
            client.delete_table(table_ref, not_found_ok=True)
            print(f"Deleted existing table {table_id}")
        except Exception as e:
            print(f"Note: Could not delete existing table: {e}")
        
        # Create new table
        table = client.create_table(table)
        print(f"Created table {table_id}")
        
        # Step 4: Load data using pandas (more reliable for complex CSV)
        df = pd.read_csv(file)
        print(f"Loaded {len(df)} rows from CSV")
        
        # Clean data
        df = df.fillna("")  # Replace NaN with empty strings
        
        # Load data to BigQuery
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        
        print(f"Successfully loaded {job.output_rows} rows into {table_ref}")
        
        # Verify
        table = client.get_table(table_ref)
        print(f"Table now contains {table.num_rows} rows")
        
        return table_ref
        
    except Exception as e:
        print(f"Error: {e}")
        return None