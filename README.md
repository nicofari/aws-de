# ProfAi Data Engineering
## AWS Module Project

### Purpose

The purpose of this project is to realize two ETL pipelines to load, transform and store in a RedShift datawarehouse, 
data related to the monero and bitcoin cryptocurrencies.

Data comes in a couple of CSV files for each currency.

### Architecture

The solution architecture is depicted in ![this diagram](https://github.com/nicofari/aws-de/blob/06216ab1f3598bfaf81622fcc630ae3b4e9c07e2/docs/Prof%20ai%20aws%20de%20flow.drawio.svg "architecture")

Data in input is uploaded to the following S3 bucket:

```buildoutcfg
s3://profai/aws-de
```

An S3 trigger is associated to the bucket, and it launches the 

#### ```s3-trigger``` lambda.

- Responsibilities

This function's duty is to monitor the upload of data files and, when both files are present, launch the specific Step Functions State Machine.

- Configuration

Environment variables:

- MONERO_FILES monero files names
- BITCOIN_FILES bitcoin files names
- MONERO_PIPELINE_ARN monero step functions state machine arn
- BITCOIN_PIPELINE_ARN bitcoin step functions state machine arn

#### ```State machine```

There are two state machines (or ETL pipelines), one for each currency:
- ```prof-ai-aws-de-bitcoin``` 
- ```prof-ai-aws-de-monero```

They have the same structure, and call the same three Glue jobs, but with different parameters:

#### Glue Etl Jobs

- raw_to_silver
- silver_to_gold
- load


