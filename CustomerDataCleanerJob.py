import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    regexp_replace, col, lower, trim, initcap, when, length, lit
)

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load input from Glue catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="customer_db_project_8",
    table_name="input_"
)

# Convert to DataFrame
df = dyf.toDF()

# ------------------------------------------
# 🔹 1. Remove duplicates
df = df.dropDuplicates()

# 🔹 2. Fill missing values
df = df.fillna({
    "name": "Unknown",
    "email": "unknown@example.com",
    "phone": "0000000000"
})

# 🔹 3. Standardize phone number (keep last 10 digits)
df = df.withColumn("phone", regexp_replace(col("phone"), r"\D", "")) \
       .withColumn("phone", col("phone").substr(-9, 10)) \
       .withColumn("phone", when(length(col("phone")) < 10, "0000000000").otherwise(col("phone")))

# 🔹 4. Standardize email (lowercase and trimmed)
df = df.withColumn("email", lower(trim(col("email"))))

# 🔹 5. Normalize name (Proper case)
df = df.withColumn("name", initcap(trim(col("name"))))

# ------------------------------------------
# Convert back to DynamicFrame
dyf_cleaned = DynamicFrame.fromDF(df, glueContext, "dyf_cleaned")

# Write cleaned data to S3
output_path = "s3://data-cleaning-raw-input/Output/"
glueContext.write_dynamic_frame.from_options(
    frame=dyf_cleaned,
    connection_type="s3",
    connection_options={"path": output_path},
    format="csv"
)

job.commit()
