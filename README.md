# Project: Data Warehouse with S3 and Redshift for Sparkify

### Summary

A music streaming company called Sparkify collects a lot of user activity data and songs details. The songs details and the user activity data from the application are currently available and stored in the format of JSON.

As Sparkify becomes more popular and increases its user base, the data goes increasing every day from MBs to GB s and it will become even more difficult for processing and analyzing the data with the JSON files. 

Hence a cloud database is necessary to store the data efficiently so that it can be used accessed easily for creating analytic reports. This project utilizes the widely used Amazon Redshift that helps in retrieval and storage of large amounts of data. A STAR Schema has been created in the Redshift Cluster.

### Schema 

The STAR schema consists of one fact table referencing any number of dimension tables which enables Sparkify to have a simplified business approach.

**Fact Table** -  songplay
<br>
**Dimension Tables** - users, songs, artists and time table
<br>
Staging Tables are also created for song and events datasets.

### Project structure
**sql_queries.py:** Contains all SQL queries of the project 
**create_tables.py:** Functions that are used to drop and create tables 
**etl.py:** Python code to execute ETL process once schemas are created in Redshift
**dwh.cfg:** All the Cluster, S3 and IAM credentials come here

### How to run

Data sources are provided by two public S3 Buckets. One bucket contains songs and artists data while the other contains all the user actions and log files.

### STEP 1 - IAM and Cluster creation
 * Create new ``IAM`` user after creating your AWS account
 * Give it AdministratorAccess and attach policies
 * Save the secret and access keys to later create clients for AWS products
 * Save the secret and access keys to later create clients for AWS products
 
 * Create a Redshift ``dc2.large``  cluster with 4 nodes 
 * Create an IAM role with the only policy attached to this as  ``AmazonS3ReadOnlyAccess``
 * Get the DWH-ENDPOINT and DWH-ROLE-ARN and insert them in the config file
 
### STEP 2 - ETL
* The ``sql_queries`` python file contains all the data normalization, transformation and table creation tasks 
* Open the terminal sessions and run `` python create_tables.py`` first
* To execute ETL process run `` python etl.py`` 

Once Tables are created, data from S3 is loaded to staging tables in Redshift Cluster. Then data is inserted into the fact and dimension tables from the staging tables.

  
