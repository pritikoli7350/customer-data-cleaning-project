##Project Description
This repository presents a scalable, serverless ETL pipeline using AWS Glue and PySpark for cleaning and standardizing raw customer data. It begins with schema discovery through AWS Glue Crawlers and processes data using efficient PySpark transformations. The pipeline addresses common data quality issues such as missing values, duplicates, and inconsistent formatting. It is designed for scalability, performance, and integration with analytics tools by writing clean, structured output back to Amazon S3. Optional data quality validations can also be included to ensure reliable results.

##CustomerDataCleanerJob.py
This is the main AWS Glue ETL script written in PySpark. It performs the following operations:
Loads raw customer data using the AWS Glue Data Catalog.
Removes duplicate records to ensure uniqueness.
Fills in missing values for key fields such as name, email, and phone.

Standardizes data by:
Lowercasing and trimming email addresses
Converting names to Proper Case
Keeping only the last 10 digits of phone numbers
Writes the cleaned data to an S3 bucket in CSV format.
Optionally includes data quality rules to validate fields and monitor cleanliness over time.
This script is optimized for large datasets and leverages Glue-specific features like job bookmarks and partitioned writes for better performance and cost-efficiency.

##customers_raw_500.csv
This sample dataset includes 500 customer records and mimics real-world data quality challenges such as:
Duplicated entries
Missing or incomplete fields
Inconsistent casing and spacing in names and emails
Invalid or noisy phone number formats
It serves as the input to the ETL pipeline, offering a realistic example of what typical raw customer data may look like.

##final_cleaned_data_corrected.csv
This is the final output of the ETL pipeline. It contains:
Cleaned, deduplicated records
Properly formatted names
Trimmed and lowercased emails
Standardized 10-digit phone numbers
This file represents high-quality, structured data that is ready for use in business intelligence dashboards, data analysis, or machine learning workflows.

