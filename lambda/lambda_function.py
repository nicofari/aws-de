import json
import urllib.parse
import boto3
import os
import json


s3 = boto3.client('s3')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    fname = os.path.basename(key).split('/')[-1]
    folder = os.path.dirname(key)

    MONERO_FILES = json.loads(os.environ['MONERO_FILES'])
    BITCOIN_FILES = json.loads(os.environ['BITCOIN_FILES'])
    MONERO_PIPELINE_ARN = os.environ('MONERO_PIPELINE_ARN')
    BITCOIN_PIPELINE_ARN = os.environ('BITCOIN_PIPELINE_ARN')
    
    is_monero = fname in MONERO_FILES
    is_bitcoin = fname in BITCOIN_FILES
    
    if not is_bitcoin and not is_monero:
        print(fname + " ignored")
        return None
    
    print("S3-trigger: got fname: " + fname)
    
    stepfunctions_client = boto3.client("stepfunctions")

    # We need to check the presence of all files before starting ETL
    if is_bitcoin:
        for f in BITCOIN_FILES:
            if (f != fname and _is_s3_object_present(bucket, folder + '/' + f)):
                print("S3-trigger: START BITCOIN PIPELINE")
                state_machine_arn = BITCOIN_PIPELINE_ARN
                try:
                    response = stepfunctions_client.start_execution(
                        stateMachineArn = state_machine_arn, 
                        input = "{}",

                    )
                except Exception as e:
                    print(f"Couldn't start state machine {state_machine_arn}. Here's why: {e}")
                
                
    if is_monero:
        for f in MONERO_FILES:
            if (f != fname and _is_s3_object_present(bucket, folder + '/' + f)):
                print("S3-trigger: START MONERO PIPELINE")
                state_machine_arn = MONERO_PIPELINE_ARN
                try:
                    response = stepfunctions_client.start_execution(
                        stateMachineArn = state_machine_arn, 
                        input = "{}",

                    )
                except Exception as e:
                    print(f"Couldn't start state machine {state_machine_arn}. Here's why: {e}")


def _is_s3_object_present(bucket, path):
    try:
        s3.get_object(Bucket=bucket, Key=path)
        return True
    except Exception as e:
        return False
