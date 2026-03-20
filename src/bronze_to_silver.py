import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.conf import SparkConf
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

# 1. Initialize Glue Job WITH Delta Configuration
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Pass the configuration into the SparkContext
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- CONFIGURATION ---
S3_BUCKET = "s3://manufacturing-quality-analytics"
BRONZE_PATH = f"{S3_BUCKET}/bronze"
SILVER_PATH = f"{S3_BUCKET}/silver"
# ---------------------

print("Starting Bronze to Silver ETL Job...")

# 2. Extract: Read raw CSVs from Bronze
df_vehicles = spark.read.csv(f"{BRONZE_PATH}/vehicles.csv", header=True, inferSchema=True)
df_stations = spark.read.csv(f"{BRONZE_PATH}/stations.csv", header=True, inferSchema=True)
df_defects = spark.read.csv(f"{BRONZE_PATH}/defects.csv", header=True, inferSchema=True)
df_logs_raw = spark.read.csv(f"{BRONZE_PATH}/inspection_logs.csv", header=True, inferSchema=True)

df_defects_ref = df_defects.select(col("defect_id").alias("ref_defect_id"))

# 3. Transform: Cleanse the dirty inspection logs using PySpark
df_logs_clean = (
    df_logs_raw
    # 1. Fill completely blank defect codes with 'NONE'
    .fillna({"defect_id": "NONE"})
    
    # 2. Force lowercase codes (like 'd001') to uppercase
    .withColumn("defect_id", upper(col("defect_id")))
    
    # 3. Join with the master reference table to spot fake codes
    .join(broadcast(df_defects_ref), col("defect_id") == col("ref_defect_id"), "left")
    
    # 4. If the code didn't match the master table (isNull), label it 'UNKNOWN'
    .withColumn("defect_id", 
                when(col("ref_defect_id").isNull(), "UNKNOWN")
                .otherwise(col("defect_id")))
    .drop("ref_defect_id") # Clean up temporary join column
    
    # 5. Fix negative inspection times by taking the absolute value
    .withColumn("inspection_duration_seconds", abs(col("inspection_duration_seconds")))
    
    # 6. Safely convert string dates into true PySpark Timestamps 
    # (Tries standard format first, falls back to DD/MM/YYYY if it fails)
    .withColumn("timestamp_clean", coalesce(
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"), 
        to_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm:ss")    
    ))
    .drop("timestamp") # Drop the old string column
    .withColumnRenamed("timestamp_clean", "timestamp") # Rename the new one
)

# --- 4. AWS GLUE DATA QUALITY CHECKS ---
print("Evaluating Data Quality...")

# Step A: Convert standard PySpark DataFrame to a Glue DynamicFrame
dyf_logs_clean = DynamicFrame.fromDF(df_logs_clean, glueContext, "dyf_logs_clean")

# Step B: Define the DQDL Ruleset
dq_rules = """
    Rules = [
        IsComplete "vin",
        ColumnLength "vin" = 17,
        IsComplete "timestamp",
        ColumnValues "inspection_duration_seconds" >= 0
    ]
"""

# Step C: Apply the checks and publish metrics to AWS CloudWatch
dyf_dq_results = EvaluateDataQuality.apply(
    frame=dyf_logs_clean,
    ruleset=dq_rules,
    publishing_options={
        "dataQualityEvaluationContext": "InspectionLogsDQ",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)
# ---------------------------------------

# 5. Load: Write all tables to Silver Layer as DELTA files
print("Writing cleansed data to Silver layer in Delta format...")

# Master tables (Clean)
df_vehicles.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/vehicles/")
df_stations.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/stations/")
df_defects.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/defects/")

# Transactional table (Cleaned + Checked)
df_logs_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/inspection_logs/")

job.commit()
print("Job Complete!")