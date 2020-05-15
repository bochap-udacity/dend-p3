# Project: Data Warehouse

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Project Organization

The project is broken multiple files that perform the following functionality.

```
.
├── README.md               ' This file that provide information about the project 
├── aws.py                  ' Contains all functions to communicate with AWS
├── create_tables.py        ' Code to drop and recreate all tables in AWS Redshift
├── data_analysis.html      ' Export of data analysis of raw data
├── data_analysis.ipynb     ' Jupyter Notebook to perform data analysis to design the data schema for this project
├── dend.yml                ' Conda environment configuration file
├── dwh.cfg                 ' Configuration file that contains all the parameters to be used to setup resources in AWS
├── etl.py                  ' Code to populate all the tables in the project with data
├── log_data                ' Folder that stores all the raw files for events
├── log_json_path.json      ' Schema file defining the raw file for events
├── secrets.template.cfg    ' Template to create a secrets.cfg file to store the credentials used to perform
├                             operations against AWS
├── setup.py                ' Code to setup all required resources in AWS
├── song_data               ' Folder that stores all the raw files for songs
├── sql_queries.py          ' Contains all queries to be expect to be executed against AWS Redshift
└── teardown.py             ' Code to remove all resources setup in AWS
```

## Prerequisites

1. AWS account
2. Conda (Miniconda or Anaconda)

## Running project

1. Run `conda env create --name dend -f dend.yml` to create Conda execution environment.
2. Run `conda activate dend` to setup conda
2. Create the IAM user in AWS that contains admin access to create new resources like Redshift Cluster
3. Store the id and key used to perform AWS API calls in secrets.cfg
4. Run `python ./setup.py` to programmatically setup AWS environment
5. Run `python ./create_tables.py` to create all the tables in AWS Redshift
6. Run `python ./etl.py` to load all the tables in AWS Redshift
7. Run `python ./teardown.py` to programmatically remove all AWS resources create for this project

## Data Schema

The schema for all the tables are created based on the analysis provided in data_analysis.ipynb.
Data types, null constraints and length constraints are add based on data observation.
Null constraints are not obeyed by Redshift but it provides a hint to the data consumer.
Primary and Foreign keys are added to allow AWS to use them as hints for query optimization.
Distribution is left as default for all dimension tables except for time as AWS will determine and switch the style based on data volume. And most of these tables are small. For the songplays fact table the key styles is selected since the volume is larger and the song_id key which is part of the query to find out songs listened by customers. 

1. staging_events
   ts is used as the dist key as it is one of the first indicators of a duplicated record when combined with other fields
2. staging_songs
   artist_id is chosen as dist key since the queries in this table involves mainly the artist to create the artist and songs dimension table. songs belong to artists
3. songplays
   song_id is chosen as the distkey since it allows more efficient query using song_id which is a key part of allowing the organization to find what about songs their customers are interested in. 
4. users
   user_id is chosen as the distkey since most queries against this table is associated to the users
5. artists
   artist_id is chosen as the distkey since most queries against this table is associated to the artists
6. time
   start_time is chosen as the distkey since most queries against this table is associated to the time

## Example Queries

Top songs played
```
 SELECT 
   COALESCE(s.title, '') song, COUNT(sp.user_id) PlayCount
 FROM 
    songplays sp
    LEFT JOIN songs s ON sp.song_id = s.song_id
 GROUP BY
 	COALESCE(s.title, '')
 ORDER BY PlayCount DESC;
```

Top songs played by gender
```
 SELECT 
   u.Gender, COALESCE(s.title, '') song, COUNT(sp.user_id) PlayCount
 FROM 
    songplays sp
    INNER JOIN users u on sp.user_id = u.user_id
    LEFT JOIN songs s ON sp.song_id = s.song_id
 GROUP BY
 	u.Gender, COALESCE(s.title, '')
 ORDER BY PlayCount DESC;
 ```