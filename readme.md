# Sparkify ETL tool 
This is a project for udacity nano degree. This ETL tool is used to help 
a startup called Sparkify to execute the process of  extract the data from dataset(json file), transform 
to specific format  
and load to Postgres database in designed structure. 

 The result of this tool is in order to help Sparkify 
to analyze the data they've been collecting on songs and user activity on
their new music streaming app. The analytics team is particularly interested
in understanding what songs users are listening to.

## Pre Requirements
Below packages should be installed.  
- pandas 
- psycopg2

## Datasets
There are two datasets need to be extracted.  
- log_data: ../../data/log_data  
    - E.g. log_data/2018/11/2018-11-12-events.json
- song_data: ../../data/song_data
    - E.g. song_data/A/B/C/TRABCEI128F424C983.json
    

## Star Schema for database
In order to make it easy for analytics, this project modling as start schema. 
Tables states below:  
![Schema](.\etl_starschema.png "Database Schema") 


## Key features
- Create table in designed schema. 
- Extract json datasets 
- Transfer the data into specific data formats which needs to be loaded to data base. 


## Running steps
1. Makesure that the datasets is in correct folder. In this case:  ../../data/log_data , ../../data/song_data

2. Run create_tables.py to create database and table. Create_tabels.py will invoke sql_queries.py automatically.  

3. Run etl.py to extract data, transfer and load to data base . 

- sql_queries.py: sql statements, create_table.py will read sql statements from this file 


