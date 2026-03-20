import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# 1. Initialize Glue Job WITH Delta Configuration
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- CONFIGURATION ---
S3_BUCKET = "s3://manufacturing-quality-analytics"
SILVER_PATH = f"{S3_BUCKET}/silver"
GOLD_PATH = f"{S3_BUCKET}/gold"
# ---------------------

print("Starting Silver to Gold ETL Job (Star Schema Build)...")

# 2. Extract: Read clean Delta tables from Silver
df_vehicles = spark.read.format("delta").load(f"{SILVER_PATH}/vehicles/")
df_stations = spark.read.format("delta").load(f"{SILVER_PATH}/stations/")
df_defects = spark.read.format("delta").load(f"{SILVER_PATH}/defects/")
df_logs = spark.read.format("delta").load(f"{SILVER_PATH}/inspection_logs/")

# --- 3. Transform Step A: Build the Dimension Tables ---
dim_vehicle = df_vehicles.select("vin", "model", "trim", "color")
dim_station = df_stations.select("station_id", "zone_name", "inspection_type")

# Assuming defects.csv has 'severity', renaming it to 'severity_level' to match your doc
# If defect_description doesn't exist in your CSV, you can remove that column here
dim_defect = df_defects.select("defect_id","severity_level")

# Build dim_date by extracting date parts from the log timestamps
# Assumes Shifts: Shift 1 (6AM-2PM), Shift 2 (2PM-10PM), Shift 3 (10PM-6AM)
dim_date = (
    df_logs.select("timestamp").distinct()
    .withColumn("date_key", date_format(col("timestamp"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("shift_number", 
        when((col("hour") >= 6) & (col("hour") < 14), 1)
        .when((col("hour") >= 14) & (col("hour") < 22), 2)
        .otherwise(3)
    )
    .drop("timestamp", "hour")
    .distinct()
)

# --- 3. Transform Step B: Build the Fact Table ---
fact_inspections = (
    df_logs
    .withColumn("date_key", date_format(col("timestamp"), "yyyyMMdd").cast("int"))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("shift_number", 
        when((col("hour") >= 6) & (col("hour") < 14), 1)
        .when((col("hour") >= 14) & (col("hour") < 22), 2)
        .otherwise(3)
    )
    .withColumn("is_defective_flag", when(col("defect_id") != "NONE", 1).otherwise(0))
    .select(
        "inspection_id", 
        "vin", 
        "station_id", 
        "defect_id", 
        "date_key", 
        "shift_number", 
        "inspection_duration_seconds", 
        "is_defective_flag"
    )
)

# --- 3. Transform Step C: Build the Aggregation Tables ---

# Prep: Join Fact with Dimensions to make aggregations easy
df_joined = (
    fact_inspections.alias("f")
    .join(dim_vehicle.alias("v"), col("f.vin") == col("v.vin"), "left")
    .join(dim_station.alias("s"), col("f.station_id") == col("s.station_id"), "left")
    .join(dim_defect.alias("d"), col("f.defect_id") == col("d.defect_id"), "left")
    .join(dim_date.alias("dt"), (col("f.date_key") == col("dt.date_key")) & (col("f.shift_number") == col("dt.shift_number")), "left")
)

# Agg 1: agg_daily_defects_by_model
agg_daily_defects_by_model = (
    df_joined.filter(col("f.is_defective_flag") == 1)
    .groupBy("f.date_key", "v.model")
    .agg(
        sum(when(col("d.severity_level") == "CRITICAL", 1).otherwise(0)).alias("critical_defects"),
        sum(when(col("d.severity_level") == "MINOR", 1).otherwise(0)).alias("minor_defects"),
        sum(when(col("d.severity_level").isNull(), 1).otherwise(0)).alias("unknown_defects"),
        count("f.inspection_id").alias("total_defects")
    )
)

# Agg 2: agg_station_bottlenecks
agg_station_bottlenecks = (
    df_joined
    .groupBy("f.date_key","s.station_id", "s.zone_name", "f.shift_number")
    .agg(
        round(avg("f.inspection_duration_seconds"), 2).alias("avg_inspection_time"),
        min("f.inspection_duration_seconds").alias("min_inspection_time"),
        max("f.inspection_duration_seconds").alias("max_inspection_time")
    )
)

# Agg 3: agg_quality_by_color
agg_quality_by_color = (
    df_joined.filter(col("f.is_defective_flag") == 1)
    .withColumn("clean_severity", coalesce(col("d.severity_level"), lit("UNKNOWN")))
    .groupBy("f.date_key","v.color", "clean_severity")
    .agg(
        count("f.inspection_id").alias("defect_count")
    )
    .withColumnRenamed("clean_severity", "severity_level")
)

# Agg 4: agg_shift_performance (Plant Manager KPI)
agg_shift_performance = (
    df_joined
    .groupBy("f.date_key", "f.shift_number")
    .agg(
        count("f.inspection_id").alias("total_cars_inspected"),
        sum("f.is_defective_flag").alias("total_defective_cars"),
        round(avg("f.inspection_duration_seconds"), 2).alias("avg_processing_time_sec")
    )
)
# ---------------------------------------------------------

# 4. Load: Write Gold tables in Delta format
print("Writing Star Schema and Aggregations to Gold layer...")

# Save Dimensions
dim_vehicle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/dim_vehicle/")
dim_station.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/dim_station/")
dim_defect.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/dim_defect/")
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/dim_date/")

# Save Fact
fact_inspections.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/fact_inspections/")

# Save Aggregations
agg_daily_defects_by_model.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/agg_daily_defects_by_model/")
agg_station_bottlenecks.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/agg_station_bottlenecks/")
agg_quality_by_color.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/agg_quality_by_color/")
agg_shift_performance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/agg_shift_performance/")

job.commit()
print("Gold Layer Star Schema Build Complete!")