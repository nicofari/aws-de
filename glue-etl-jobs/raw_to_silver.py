import sys
import boto3
from awsglue.utils import getResolvedOptions
import pandas as pd
import awswrangler as wr
import os

args = getResolvedOptions(sys.argv,
                          ['JobName',
                           'price_file_name',
                           'trend_file_name'
                           ])
             
job_name = args['JobName']
price_file_name = args['price_file_name']
trend_file_name = args['trend_file_name']

print(f"{job_name}: the price_file_name is: {price_file_name}")
print(f"{job_name}: the trend_file_name is: {trend_file_name}")

s3 = boto3.resource('s3')

# FIXME: transform these constants below into parameters
bucketname = "kotik-profai"
source = "aws-de/raw"
target = "aws-de/silver"

bucket = s3.Bucket(bucketname)

def _get_file_name_without_extension(file_name):
    return os.path.basename(file_name).split('.')[0]

for fn in [price_file_name, trend_file_name]:
    try:
        obj = s3.Object(bucketname, source + '/' + fn)
    except Exception as e:
        print(f"error get object: {e}")
        raise e
    
    print(f"{job_name}: loading file {fn}")

    fn_parquet = _get_file_name_without_extension(fn) + '.parqet'
    target_filename = f"s3://{bucketname}/{target}/{fn_parquet}"
    
    print(f"{job_name} target_filename: {target_filename}")
    
    # Load and (first) transformations
    if fn == price_file_name:
        df = pd.read_csv(obj.get()['Body'], parse_dates=['Date'], thousands=',')
        # Get rid of some columns we are not interested in
        df = df.drop(axis=1, columns=['Open', 'High', 'Low', 'Vol.', 'Change %'])
        # Find record with invalid price and fill them with rolling mean
        df.loc[df['Price'] == -1, 'Price'] = df['Price'].rolling(window=5, min_periods=1).mean()
    else:
        df = pd.read_csv(obj.get()['Body'], parse_dates=['Settimana'], thousands=',')
        # Normalize column names
        df.columns.values[0] = 'Week'
        df.columns.values[1] = 'Google_trend'

    wr.s3.to_parquet(
        df = df,
        path = target_filename
    )

    print(f"{job_name}: about to delete {fn}")
# TODO uncomment the below line before final delivery    
#    obj.delete()

