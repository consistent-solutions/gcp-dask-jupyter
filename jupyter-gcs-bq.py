#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install google.cloud.bigquery


# In[2]:


from google.cloud import bigquery as bq


# In[3]:


import time


# In[4]:


pip install gcsfs


# In[5]:


import gcsfs


# In[6]:


import pandas as pd


# In[7]:


import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "jupyter-to-bq.json"


# In[8]:


bq_client = bq.Client()


# In[9]:


taxi_query="""
#standardSQL
SELECT *
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017`
LIMIT 2000000
"""

job_config = bq.QueryJobConfig()
now=time.time()
query_job=bq_client.query(taxi_query,location='US')
res=query_job.result()
print('query took:',round(time.time()-now,2),'s')


# In[10]:


from dask.distributed import Client
import dask.dataframe as dd

dask_client = Client()
dask_client


# In[33]:


#### Load data from BigQuery Table to GCS bucket as multiple (1GB) csv files
##
## NOTE: must first create a parent Dataset in project and copy over the 
##       public Data(Table) to a Table in your project ("attached" to the parent Dataset).
##
## NOTE: jupyter-to-bigquery sa, must have 3 roles
##        1. BigQuery Data Owner
##        2. BigQuery JobUser
##        3. Storage Object Creator
#
# NOTE: command-line equivalent:
#     bq --location=US extract --destination_format=CSV --print_header=false 'gke-day-two.new_york_taxi_trips.tlc_yellow_trips_2018' gs://test-jupyter-gcs-bq-creds/data-*.csv
#     (https://cloud.google.com/bigquery/docs/bq-command-line-tool)
#

# from google.cloud import bigquery
# bq_client = bigquery.Client()
bucket_name = 'test-jupyter-gcs-bq-creds'
project = "gke-day-two"
dataset_id = "new_york_taxi_trips"
table_id = "tlc_yellow_trips_2018"

destination_uri = "gs://{}/{}".format(bucket_name, "data-*.csv")
dataset_ref = bq.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table(table_id)

extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="US",
)  # API request
extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
)


# In[35]:


#import dask.dataframe as dd
#df = dd.read_gbq("gke-day-two.new_york_taxi_trips.tlc_yellow_trips_2018") # proposed design (but not yet supported)
#df


# In[14]:


## This cell alone will not appear to do anything UNTIL you do an operation on the dask dataframe (next cell)
import time

now=time.time()

project = "gke-day-two"
bucket_name = 'test-jupyter-gcs-bq-creds'

gcs = gcsfs.GCSFileSystem(project=project,token=os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) 
destination_uri = "gs://{}/{}".format(bucket_name, "data-*.csv")

df = dd.read_csv(destination_uri, parse_dates=['pickup_datetime','dropoff_datetime'])

print('read csv took:',round(time.time()-now,2),'s')


# In[16]:


len(df) # this takes a while (need to work on tuning # of workers and other params)


# In[ ]:




