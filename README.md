## Data Lakes with Spark
This project builds a data lake and an ETL pipline using Spark. Data is stored in AWS S3 and transformed in AWS EC2.

## This Repository Includes:
- etl.py reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3. Each of the five tables are written to parquet files in a separate analytics directory on S3.
- The dl.cfg holds the configurations of the AWS IAM user.
- Data folder includes some test data