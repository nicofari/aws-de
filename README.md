# ProfAi Data Engineering
## AWS Module Project

### Purpose

The purpose of this project is to realize two ETL pipelines to load, transform and store in a RedShift datawarehouse, 
data related to the monero and bitcoin cryptocurrencies.

Data comes in a couple of CSV files for each currency.

### Architecture

The solution architecture is depicted in the below diagram: ![this diagram](https://github.com/nicofari/aws-de/blob/06216ab1f3598bfaf81622fcc630ae3b4e9c07e2/docs/Prof%20ai%20aws%20de%20flow.drawio.svg "architecture")

When data are uploaded to the following S3 bucket:

```buildoutcfg
s3://profai/aws-de
```

An S3 trigger launches the 

#### ```s3-trigger``` lambda.

This function is responsible is to monitor the upload of *both* data files for each currency, to avoid starting ETL when the set isn't complete.

When both files are present, the specific Step Functions State Machine Pipeline is started.

- Configuration

File names for each currency, and pipeline ARN's are stored in these environment variables of the Lambda:

- MONERO_FILES monero files names (as json array)
- BITCOIN_FILES bitcoin files names
- MONERO_PIPELINE_ARN monero step functions state machine arn
- BITCOIN_PIPELINE_ARN bitcoin step functions state machine arn

#### ```State machine```

There are two state machines (or ETL pipelines), one for each currency:
- ```prof-ai-aws-de-bitcoin``` 
- ```prof-ai-aws-de-monero```

They have the same structure, shown below:
![this diagram](https://github.com/nicofari/aws-de/blob/0f5d3b78faa6bc05455749cde4607955a2beabb7/docs/stepfunctions_graph.png "pipeline flow")

The first step is
- raw_to_silver

In this state price missing data are imputed, and google trend column names are normalized. 
Data are then converted to parquet format and stored in the ```silver``` bucket.

- silver_to_gold

In this state data are read from silver bucket and joined into one (after applying a median to price values)

- load

In this state data are loaded to a Redshift cluster.

- SNS Publish

A message is send to a SNS Topic to notify interested parties of the completed data loading.

The two pipelines are actually one, the same one duplicated with different Etl job parameters, for the specific data file names.

#### Glue Etl Jobs

- raw_to_silver
- silver_to_gold
- load


