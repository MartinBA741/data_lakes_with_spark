# Data Lakes with Spark
This project builds a data lake and an ETL pipline using Spark. The ETl  moves user- and song data from an exciting data warehouse in S3 to a data lake.  
The ETL pipeline processes data using Spark enabeling the pipeline to be deployed on a distributed cluster such as AWS Elastic MapReduce (EMR).

## This Repository Includes:
* The etl.py script reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3. Each of the five tables are written to parquet files in a separate analytics directory on S3.
* The dl.cfg holds the configurations of the AWS IAM user.
* The data folder includes some test data. The full dataset is stored on S3

## Data Lake Design
The data lake is desgined such that the following tables are partitioned by the corresponding attributes:
* songs_table: year and artist_id
* time_table: year and month
* songsplay_table: year and month

## How to run this repo
In order to run the script:
* Firstly, open the dl.cfg to write your credentials in AWS. 
* After that, open a terminal and connect to AWS and create an EMR cluster. For more info see e.g. https://www.oreilly.com/content/how-do-i-connect-to-my-amazon-elastic-mapreduce-emr-cluster-with-ssh/
* Lastly, submit etl.py 