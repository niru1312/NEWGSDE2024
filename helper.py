import pandas as pd
import boto3
import gzip
from io import BytesIO

def check_datatype(df):
    return df.dtypes

def check_notnull(df):
    rows_with_null = df[df.isnull().any(axis=1)]
    if len(rows_with_null) == 0:
        print('Dataframe is NOT NULL')
    else:
        print('Dataframe is NULL')
    return None

def check_notunique(df,column_name):
    num_unique_values_A = df[column_name].nunique()
    if num_unique_values_A == len(df[column_name]):
        print("Column", column_name, "has unique values")
    else:
        print("Column", column_name, "has duplicate values")
    return None

s3_client = boto3.client('s3')
bucket_name = 'salesdataforgsde2404'
response = s3_client.list_objects_v2(Bucket=bucket_name)
def Get_File_Data(bucketname,Key):
    obj = s3_client.get_object(Bucket=bucket_name,Key=Key)
    with gzip.open(obj['Body'], 'rt') as f:
        df=pd.read_csv(f,sep='|')
    return df