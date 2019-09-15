# Data Lakes with Spark

The purpose of this project was to build an ETL process using Spark (PySpark) on an AWS EMR Cluster. Provided data in .JSON-format had to be extracted, analyzed, transformed and the loaded into parquet files representing tables of data lake (dimensional model). Both input and output files were stored on AWS S3 buckets.

## Project files

- dl.cfg: stores AWS credentials that are needed when using files in a S3 bucket
- etl.py: reads data from a S3 buckets, transforms that data using Spark and writes them to S3.

## How to run

1. Configure dl.cfg file with AWS credentials.
2. Run python etl.py