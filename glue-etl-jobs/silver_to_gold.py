import sys
import boto3
from awsglue.utils import getResolvedOptions
import pandas as pd
import awswrangler as wr
import os

args = getResolvedOptions(sys.argv,
                          ['JobName',
                           'price_file_name',
                           'trend_file_name',
                           'output_file_name'
                           ])
             
job_name = args['JobName']
price_file_name = args['price_file_name']
trend_file_name = args['trend_file_name']
output_file_name = args['output_file_name']

print(f"{job_name}: the price_file_name is: {price_file_name}")
print(f"{job_name}: the trend_file_name is: {trend_file_name}")
print(f"{job_name}: the output_file_name is: {output_file_name}")

s3 = boto3.resource('s3')

# FIXME: transform these constants below into parameters
bucketname = "kotik-profai"
source = "aws-de/silver"
target = "aws-de/gold"

bucket = s3.Bucket(bucketname)

def _get_file_name_without_extension(file_name):
    return os.path.basename(file_name).split('.')[0]

for fn in [price_file_name, trend_file_name]:
    try:
        obj = s3.Object(bucketname, source + '/' + fn)
    except Exception as e:
        print(f"{job_name}: error get object: {e}")
        raise e
    
    source_path = f"s3://{bucketname}/{source}/{fn}"
    
    print(f"{job_name}: loading file {fn}")

    target_filename = f"s3://{bucketname}/{target}/{output_file_name}"
    
    print(f"{job_name} output_file_name: {target_filename}")
    
    if fn == price_file_name:
        df_price = wr.s3.read_parquet(path=source_path)
        # Calculate a moving mean
        df_price['Price'] = df_price['Price'].rolling(window=5, min_periods=1).mean()
    else:
        df_trend = wr.s3.read_parquet(path=source_path)

    print(f"{job_name}: about to delete {fn}")
# TODO uncomment the below line before final delivery    
#    obj.delete()

# Merge the two datasets
df_merge = pd.merge(df_trend, df_price, left_on="Week", right_on="Date")
# Get rid of duplicated column "Date"
df_merge = df_merge.drop(axis = 1, columns = ['Date'])

wr.s3.to_parquet(
    df = df_merge,
    path = target_filename
)


