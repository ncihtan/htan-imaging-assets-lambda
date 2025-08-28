import boto3
import time
import csv
import json
from io import StringIO
from datetime import datetime, timedelta

# Athena and S3 configurations
athena_client = boto3.client("athena")
s3_client = boto3.client("s3")

DATABASE = "default"
OUTPUT_LOCATION = "s3://htan-assets/inventory/query-results/"
JSON_OUTPUT_LOCATION = "s3://htan-assets/inventory/final-output/"

# Generate yesterday's date
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
table_location = f"s3://htan-assets/htan-assets-inventory-pq/htan-assets/htan-assets-inventory-pq/hive/dt={yesterday}-01-00/"

# Queries
DROP_TABLE_QUERY = "DROP TABLE IF EXISTS s3inventorytable;"

CREATE_TABLE_QUERY = f"""
CREATE EXTERNAL TABLE s3inventorytable(
    Bucket string, 
    Key string, 
    Size string, 
    LastModifiedDate string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '{table_location}';
"""

FINAL_QUERY = f"""
WITH minerva AS (
    SELECT 
        regexp_extract(key, '(syn\\d+)', 1) AS synid,
        CONCAT('https://d3p249wtgzkn5u.cloudfront.net/', key) AS minerva,
        ROW_NUMBER() OVER (PARTITION BY regexp_extract(key, '(syn\\d+)', 1) 
            ORDER BY lastmodifieddate DESC) AS rn
    FROM s3inventorytable 
    WHERE key LIKE '%index.html'
),
thumb AS (
    SELECT 
        regexp_extract(key, '(syn\\d+)', 1) AS synid,
        CONCAT('https://d3p249wtgzkn5u.cloudfront.net/', key) AS thumbnail,
        ROW_NUMBER() OVER (PARTITION BY regexp_extract(key, '(syn\\d+)', 1) 
            ORDER BY lastmodifieddate DESC) AS rn
    FROM s3inventorytable 
    WHERE key LIKE '%thumbnail.%'
)
SELECT 
    COALESCE(m.synid, t.synid) AS synid,
    m.minerva,
    t.thumbnail
FROM minerva m
FULL OUTER JOIN thumb t ON m.synid = t.synid
WHERE (m.rn = 1 OR m.rn IS NULL) AND (t.rn = 1 OR t.rn IS NULL);
"""


# Start query execution
def start_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )
    return response["QueryExecutionId"]


# Wait for query completion
def wait_for_query_completion(query_execution_id):
    while True:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = response["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return state
        time.sleep(2)  # Check every 2 seconds


# Convert CSV result to JSON, removing empty values
def convert_csv_to_json(query_execution_id):
    s3_bucket = OUTPUT_LOCATION.split("/")[2]
    s3_prefix = "/".join(OUTPUT_LOCATION.split("/")[3:])
    csv_key = f"{s3_prefix}{query_execution_id}.csv"
    json_key = f"final-output/htan-imaging-assets-latest.json"

    # Fetch CSV file
    csv_obj = s3_client.get_object(Bucket=s3_bucket, Key=csv_key)
    csv_content = csv_obj["Body"].read().decode("utf-8")

    # Convert CSV to JSON, dropping empty values
    csv_reader = csv.DictReader(StringIO(csv_content))
    json_data = []
    for row in csv_reader:
        filtered_row = {
            k: v for k, v in row.items() if v
        }  # Remove keys with empty values
        json_data.append(filtered_row)

    # Write JSON to S3
    s3_client.put_object(
        Bucket=s3_bucket, Key=json_key, Body=json.dumps(json_data, indent=4)
    )
    return f"s3://{s3_bucket}/{json_key}"


def lambda_handler(event, context):
    # Drop old table
    drop_id = start_query(DROP_TABLE_QUERY)
    if wait_for_query_completion(drop_id) != "SUCCEEDED":
        raise Exception("Failed to drop the old table")

    # Create new table
    create_id = start_query(CREATE_TABLE_QUERY)
    if wait_for_query_completion(create_id) != "SUCCEEDED":
        raise Exception("Failed to create the table")

    # Run final query
    final_id = start_query(FINAL_QUERY)
    if wait_for_query_completion(final_id) != "SUCCEEDED":
        raise Exception("Failed to run the final query")

    # Convert result to JSON
    json_location = convert_csv_to_json(final_id)

    return {
        "statusCode": 200,
        "body": f"Athena query results saved as JSON at {json_location}",
    }
