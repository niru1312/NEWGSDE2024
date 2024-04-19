"""
This module is entry point for application as well as contains prepare and transform funnctions
"""
import pandas as pd
import boto3
import gzip
from io import BytesIO

#connect to S3 using boto3
s3_client = boto3.client('s3')
bucket_name = 'salesdataforgsde2404'
response = s3_client.list_objects_v2(Bucket=bucket_name)
files = [obj['Key'] for obj in response.get('Contents', [])]

#Once connect to S3 Bucket, Connect and get the data from the files which are in gzip and create dataframe
def Get_File_Data(bucketname,Key):
    obj = s3_client.get_object(Bucket=bucket_name,Key=Key)
    with gzip.open(obj['Body'], 'rt') as f:
        df=pd.read_csv(f,sep='|')
    return df

#Initilize  Dataframes for each file in S3 bucket
df_averagecosts = Get_File_Data('salesdataforgsde2404','fact.averagecosts.dlm.gz')
df_transactions = Get_File_Data('salesdataforgsde2404','fact.transactions.dlm.gz')
df_clnd = Get_File_Data('salesdataforgsde2404','hier.clnd.dlm.gz')
df_hldy= Get_File_Data('salesdataforgsde2404','hier.hldy.dlm.gz')
df_invloc= Get_File_Data('salesdataforgsde2404','hier.invloc.dlm.gz')
df_invstatus= Get_File_Data('salesdataforgsde2404','hier.invstatus.dlm.gz')
df_possite= Get_File_Data('salesdataforgsde2404','hier.possite.dlm.gz')
df_pricestate= Get_File_Data('salesdataforgsde2404','hier.pricestate.dlm.gz')
df_prod= Get_File_Data('salesdataforgsde2404','hier.prod.dlm.gz')
df_rtlloc = Get_File_Data('salesdataforgsde2404','hier.rtlloc.dlm.gz')


#Create a function to check the if the dataframe is null or it exists data, Here we are checking for record
def check_notnull(df):
    rows_with_null = df[df.isnull().any(axis=1)]
    if len(rows_with_null) == 0:
        print('Dataframe is NOT NULL')
    else:
        print('Dataframe is NULL')
    return None

#Run the Check now null function to chec the dataframe
print("Checking for NON-NULL Values")

print("\nAveragecosts")
check_notnull(df_averagecosts)

print("\nTransactions")
check_notnull(df_transactions)

print("\nDataframe clnd")
check_notnull(df_clnd)

print("\nhldy")
check_notnull(df_hldy)

print("\ninvloc")
check_notnull(df_invloc)

print("\nTinvstatus")
check_notnull(df_invstatus)

print("\npossite")
check_notnull(df_possite)

print("\npricestate")
check_notnull(df_pricestate)

print("\nTprod")
check_notnull(df_prod)

print("\nrtlloc")
check_notnull(df_rtlloc)

#Check for not unique data in columns

def check_notunique(df):
    column_names = []
    column_names = df.columns.tolist()
    for col in column_names:
        num_unique_values = df[col].nunique()
        if num_unique_values == len(df[col]):
            print("Column", col, "has unique values")
        else:
            print("Column", col, "has duplicate values")
    return None

#Checking for the unique data in the columns for each dataframe
print("\nChecking df_averagecosts Dataframe")
check_notunique(df_averagecosts)

print("\nChecking df_transactions Dataframe")
check_notunique(df_transactions)

print("\nChecking df_clnd Dataframe")
check_notunique(df_clnd)

print("\nChecking df_hldy Dataframe")
check_notunique(df_hldy)

print("\nChecking df_invloc Dataframe")
check_notunique(df_invloc)

print("\nChecking df_invstatus Dataframe")
check_notunique(df_invstatus)

print("\nChecking df_possite Dataframe")
check_notunique(df_possite)

print("\nChecking df_pricestate Dataframe")
check_notunique(df_pricestate)

print("\nChecking df_prod Dataframe")
check_notunique(df_prod)

print("\nChecking df_rtlloc Dataframe")
check_notunique(df_rtlloc)

#Check the datatypes of the columns
def check_datatype(df):
    return df.dtypes

#Run the function to check the datatypes of columns in dataframe
print("\n Check the DataTypes for Averagecosts DataFrame")
print(check_datatype(df_averagecosts))
print("\n Check the DataTypes for df_transactions DataFrame")
print(check_datatype(df_transactions))
print("\n Check the DataTypes for df_clnd DataFrame")
print(check_datatype(df_clnd))
print("\n Check the DataTypes for df_hldy DataFrame")
print(check_datatype(df_hldy))
print("\n Check the DataTypes for df_invloc DataFrame")
print(check_datatype(df_invloc))
print("\n Check the DataTypes for df_invstatus DataFrame")
print(check_datatype(df_invstatus))
print("\n Check the DataTypes for df_possite DataFrame")
print(check_datatype(df_possite))
print("\n Check the DataTypes for df_pricestate DataFrame")
print(check_datatype(df_pricestate))
print("\n Check the DataTypes for df_prod DataFrame")
print(check_datatype(df_prod))
print("\n Check the DataTypes for df_rtlloc DataFrame")
print(check_datatype(df_rtlloc))


#Create a refined table called mview_weekly_sales which aggregates sales_units, sales_dollars, and disocunt_dollars
# by pos_site_id, sku_id, fsclwk_id, price_substate_id and type.

#We have to JOIN transactions and CLND dataframe in orer to get the fsclwk_id field from clnd dataframe
merged_df = pd.merge(df_transactions, df_clnd, on='fscldt_id')

#GROUP BY POS_SITE_ID, SKU_ID, FSCLWK_ID, PRICE_SUBSTATE_ID, TYPE and aggregate SALES_UNIT, SALES_DOLLARS, DISCOUNT_DOLLARS
mview_weekly_sales = merged_df.groupby(['pos_site_id', 'sku_id', 'fsclwk_id', 'price_substate_id', 'type']).agg(
    total_sales_units=('sales_units', 'sum'),
    total_sales_dollars=('sales_dollars', 'sum'),
    total_discount_dollars=('discount_dollars', 'sum')
).reset_index()

output_file = "D:\demo\pythonProject1\GSDE2024\output\mview_weekly_sales.csv"
mview_weekly_sales.to_csv(output_file, index=False)
print(f'Data has been exported to {output_file}')