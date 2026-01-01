import sys
import json
import logging
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# -------------------------
# Initialize Glue Context
# -------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------
# Logging Configuration
# -------------------------
logger = logging.getLogger("project3-rds-to-s3")
logger.setLevel(logging.INFO)

logger.info("Starting Glue job: RDS → S3 ingestion")

# -------------------------
# Constants
# -------------------------
SECRET_NAME = "project3-rds-secret"
REGION_NAME = "us-east-1"
S3_BASE_PATH = "s3://project3-bronze-bucket/raw-data/"
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# -----------------------------------------
# Get DB Credentials from Secrets Manager
# -----------------------------------------
def get_secret(secret_name, region):
    try:
        client = boto3.client("secretsmanager", region_name=region)
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        logger.info("Successfully retrieved secret from Secrets Manager")
        return secret
    except Exception as e:
        logger.error("Failed to retrieve secret", exc_info=True)
        raise e

secret = get_secret(SECRET_NAME, REGION_NAME)

jdbc_url = (
    f"jdbc:sqlserver://{secret['host']}:{secret['port']};"
    f"databaseName={secret['dbname']};"
    "encrypt=true;trustServerCertificate=true"
)

connection_properties = {
    "user": secret["username"],
    "password": secret["password"],
    "driver": JDBC_DRIVER
}

# ------------------------------------
# Get All Tables from SQL Server
# ------------------------------------
def get_table_list():
    query = """
    (SELECT TABLE_SCHEMA, TABLE_NAME
     FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_TYPE = 'BASE TABLE') AS tables
    """

    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=connection_properties
    )

    tables = [
        f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
        for row in df.collect()
    ]

    logger.info(f"Found {len(tables)} tables in RDS")
    return tables

# -------------------------
# Read Table → Write to S3
# -------------------------
def ingest_table(table_name):
    try:
        logger.info(f"Reading table: {table_name}")

        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_properties
        )

        target_path = f"{S3_BASE_PATH}{table_name.replace('.', '_')}/"

        logger.info(f"Writing table {table_name} to {target_path}")

        df.write.mode("overwrite").parquet(target_path)

        logger.info(f"Successfully ingested {table_name}")

    except Exception as e:
        logger.error(f"Failed to ingest table {table_name}", exc_info=True)

# -------------------------
# Main Execution
# -------------------------
try:
    tables = get_table_list()

    for table in tables:
        ingest_table(table)

    logger.info("Glue job completed successfully")

except Exception as e:
    logger.critical("Glue job failed", exc_info=True)
    raise e
