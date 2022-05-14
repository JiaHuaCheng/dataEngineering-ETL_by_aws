## Data Enginerring Project - Data Warehouse

As a data engineer, our goal is to build a scalable, reliable pipeline to process raw data into high level view tables. With well-organized datasets, buiness analysis team would be easier to dig out the value from the data we prepared. 

Consider a scenario given as below. A music streaming startup, Sparkify, has very strong annual growth on their user base. The database they built in the beginning is not appropricate for their buniess model anymore. As their data engineer, we plan to migrate all our data to cloud because of its cost-effective, and scalability. All our previous data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this work, we build a native ETL pipeline that extracts our data from S3, stages them in Redshift, and transforms data into a set of dimensional tables. The procedure can be divided into the following steps: 

1. Loading data from S3 to Redshift. (Preparation)
- Assume S3 and Redshift have all been setup. We connect to Redshift database cluster by psycopg2.
- Create staging tables, and dimension tables in the Redshift cluster.

2. Creating final analytics tables from our staging tables. (ETL)
- Prepare staging tables by "copy" command. We copy JSON log from s3 to our staging tables. (Copy is far effieient than insertion row by row.) 
- We choose to use star-schema to reorganize staging tables into our final fact table and dimension tables.

## Usage

Execute the following command in your command line. It might takes 10-15 mins for running etl.py.

```python
~ % python create_tables.py
~ % python etl.py
```

## Database schema design and ETL pipeline

Note that, analysis process might involve several tables together using "join". "Join" is a time-comsuming opration so that, as a data engineer, we have to design our database schema carefully to balance between the performance and data redundancy.

1. staging tables
We basically follow all the attributes in JSON file to build our staging tables. It's straightforward so I'll skip this part.

2. final analytics tables
We use star-schema for this work. The fact table is used to record bussiness event. In our case, the event is being recorded whenever a user plays a song.
-- Fact Table (songplays)
-- Dimension Table (users, songs, times, artists)

![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/star-scheme.png)  

Here, we attach some scheme format, and part of its query result for reference.  

Fact table - songplays
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/songplays-schema.png)  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/songplays-query-result.png)  

Dimension table - users  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/users-schema.png)  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/users-query-result.png)  

Dimension table - times  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/times-schema.png)  
![alt text](https://github.com/JiaHuaCheng/dataEngineering-ETL_by_aws/blob/main/img/times-query-result.png)  

## File Descriptions

1. create_tables.py - A python script for creating Postgres tables (Follow star-schema to create tables).
2. etl.py - A stardard ETL process script (load from staging table and insert into fact/dimension table).  
3. sql_queries.py - A python script for creating, inserting, and dropping table. We follow star-schema.
4. dwg.cfg - The config file contains AWS login information.
5. README.md - The description File including usage, purpose and summary.
