import os
from google.cloud import bigquery
import json


class Bq_tools:
    def __init__(self):
        self.PROJECT_ID = os.environ["PROJECT_ID"]           
        self.DATASET_ID = "news"
        self.TABLE_ID   = "news_data"
        self.client = bigquery.Client(project=self.PROJECT_ID)
        self.table_ref = f"`{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID}`"

    def execute_sql_query(self,sql):
        query_job = self.client.query(sql)           
        df = query_job.to_dataframe()
        data=df.to_dict(orient='records')
        return data

    def get_schema(self):
        sql = f"""
        SELECT
        column_name  AS name,
        data_type    AS type
        FROM `{self.PROJECT_ID}.{self.DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{self.TABLE_ID}'
        ORDER BY ordinal_position
        """
        schema=self.execute_sql_query(sql)
        return json.dumps({'project_id':self.PROJECT_ID ,'dataset_id':self.DATASET_ID,'table_id':self.TABLE_ID}) + 'Schema:\n' + json.dumps(schema)
    
    def get_news_by_category(self,category:str)->str:
        sql = f"""
        SELECT
        headline,extracted_text,sentiment,entities
        FROM `{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID}`
        where CONTAINS_SUBSTR(detailed_category, '{category}') 
        OR CONTAINS_SUBSTR(category, '{category}') LIMIT 5
        """
        return self.execute_sql_query(sql)
    
    def get_news_by_search_term(self,search_term:str)->str:
        sql = f"""
        SELECT
        headline,extracted_text,sentiment,entities
        FROM `{self.PROJECT_ID}.{self.DATASET_ID}.{self.TABLE_ID}`
        where CONTAINS_SUBSTR(entities, '{search_term}') 
         LIMIT 5
        """
        return self.execute_sql_query(sql)

toolkit=Bq_tools()