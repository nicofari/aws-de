# ProfAi Data Engineering
## AWS Module Project

# Purpose

The purpose of this project is to realize two ETL pipelines to load, transform and store in a RedShift datawarehouse, 
data related to the monero and bitcoin cryptocurrencies.

Data comes in a couple of CSV files for each currency.

# Architecture

Data in input is uploaded to the following S3 bucket:

```buildoutcfg
s3://profai/aws-de
```

Then, following the [Medaillon Architecture](https://dataengineering.wiki/Concepts/Medallion+Architecture),
there are three subfolders in it:

- ```raw```
- ```silver```
- ```gold```

Two pipelines (more precisely two```AWS Step Functions``` state machines) are created, one for each currency:

- ```prof-ai-aws-de-bitcoin``` 
- ```prof-ai-aws-de-monero```

When files are uploaded in the raw folder an S3 trigger launches a Lambda function.

Responsibility of this function is to decide when both files of each couple are present.

When this happens the Lambda invokes the corresponding state machine flow.

File names and state machines ARNs are environment variables for the lambda.

