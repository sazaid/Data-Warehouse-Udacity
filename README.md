# Data-Warehouse-Udacity
**purpose of this database:**

# Sparkify is a music streaming startup with a growing user base and song database. Their user activity and songs metadata data resides in json files in S3. The goal of the project is to build an ETL pipeline that will extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


**Create Table Schemas And Build ETL Pipeline**

# Write a SQL CREATE statement for each tables in sql_queries.py
# Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist.
# Launch a redshift cluster and create an IAM role with an AdministratorAccess and take the access key and secret that has read access to S3.Add redshift database and IAM role info to dwh.cfg

# Test by running create_tables.py and checking the table schemas in your redshift database. 



# Finally I Run the create_tables script to set up the database staging and analytical tables and run the etl script to extract data from the files in S3, stage it in redshift, and finally store it in the dimensional tables.


# Delete cluster and role. 
