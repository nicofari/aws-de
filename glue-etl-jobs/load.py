import redshift_connector
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          ['JobName',
                           'source_table_name',
                           'dest_table_name',
                           'host',
                           'database',
                           'username',
                           'password',
                           'port',
                           'iam_role'
                           ])
             
job_name = args['JobName']
source_table_name = args['source_table_name']
dest_table_name = args['dest_table_name']

host = args['host']
database = args['database']
username = args['username']
password = args['password']
port = int(args['port'])
iam_role = args['iam_role']

print(f"{job_name}: the source_table_name is: {source_table_name}")
print(f"{job_name}: the dest_table_name is: {dest_table_name}")
print(f"{job_name}: the connection string is: {username}@{host}:{port}/{database}")

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
    