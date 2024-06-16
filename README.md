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

This function's responsibility is to monitor the upload of *both* data files for each currency, to avoid starting ETL when the set isn't complete.

When both files are present, the specific Step Functions State Machine Pipeline is started.

#### Configuration

File names for each currency, and pipeline ARN's are stored in these environment variables of the Lambda:

- MONERO_FILES monero files names (as json array)
- BITCOIN_FILES bitcoin files names
- MONERO_PIPELINE_ARN monero step functions state machine arn
- BITCOIN_PIPELINE_ARN bitcoin step functions state machine arn

#### ETL Pipelines 

Pipelines are implemented leveraging ```AWS Step Functions``` State Machine concept.

There are two state machines, one for each currency:
- ```prof-ai-aws-de-bitcoin``` 
- ```prof-ai-aws-de-monero```

Since the transformations are the same for both currencies, the only difference being file names, pipelines do  the same structure, which is shown below:
![this diagram](https://github.com/nicofari/aws-de/blob/0f5d3b78faa6bc05455749cde4607955a2beabb7/docs/stepfunctions_graph.png "pipeline flow")

Different file names are expressed through the use of job parameters in each state.

The first state is
- raw_to_silver

In this step price missing data are imputed, and google trend column names are normalized. 
Data are then converted to parquet format and stored in the ```silver``` bucket.

- silver_to_gold

In this phase data are read from silver bucket and joined into one (after applying a median to price values) dataframe which is stored into the ```gold``` bucket.

- load

In this state data are loaded to a Redshift cluster.

- SNS Publish

A message is send to a ```SNS Topic``` to notify interested parties of the completed data loading.

The two pipelines are actually one, the same one duplicated with different Etl job parameters, for the specific data file names.

#### Glue Etl Jobs

ETL Jobs are implemented in ```AWS Glue``` as Python procedures.

- raw_to_silver

Relevant normalization part is:
```
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
```

- silver_to_gold

Relevant transformation code is:
```
    if fn == price_file_name:
        df_price = wr.s3.read_parquet(path=source_path)
        # Calculate a moving mean
        df_price['Price'] = df_price['Price'].rolling(window=5, min_periods=1).mean()
    else:
        df_trend = wr.s3.read_parquet(path=source_path)
```

- load

```
conn = redshift_connector.connect(
     host = host,
     database = database,
     port = port,
     user = username,
     password = password
)

conn.autocommit = True

with conn.cursor() as cursor:
    cursor.execute(f"TRUNCATE {dest_table_name}")
    cursor.execute(f"COPY {dest_table_name} FROM '{source_table_name}' iam_role '{iam_role}' FORMAT AS PARQUET;")
```

Each job has a set of parameters for source and destination tables, and for database credentials.

#### Redshift and Quicksight

Once in Redshift, data can be visualized in Quicksight.

Example diagrams below:

Monero:
![this diagram](https://github.com/nicofari/aws-de/blob/a320495f460264b7124536ccc2848a319d5ffed3/docs/Quicksight_monero.jpg "monero trend diagram")

Bitcoin:
![this diagram](https://github.com/nicofari/aws-de/blob/a320495f460264b7124536ccc2848a319d5ffed3/docs/bitcoin_quicksight.png "bitcoin trend diagram")
