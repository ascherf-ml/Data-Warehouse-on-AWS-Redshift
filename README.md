# Project: Data Warehouse
## Introduction
The music streaming startup, Sparkify, has data that resides in S3, in a directory of JSON logs on user activity on the app,
as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am building an ETL pipeline that extracts their data from S3,
stages them in Redshift, and transforms data into a star schema  with a set of dimensional tables and a fact table for their analytics
team to continue finding insights in what songs their users are listening to. 

## Project Description
### Project files
```sh
dwh.cfg
create_redshift.py
check_redshift_status.py
connect_to_redshift.py
delete_redshift.py
sql_queries.py
create_tables.py
etl.py
README.md
```

**dwh.cfg**
Main file for the delivery of parameters to create an IAM role and redshift cluster and to connect to the S3 server.
>[REDSHIFT_CLUSTER]
Has information on:
HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, CLUSTER_IDENTIFIER, CLUSTER_TYPE, NODE_TYPE, NODE_COUNT

>[AWS]
Has information on:
KEY, SECRET, AWS_REGION

>[IAM_ROLE]
Has information on:
NAME
POLICY_NAME, ARN, REDSHIFT_ARN

>[S3]
LOG_DATA, LOG_JSONPATH, SONG_DATA

**create_redshift.py**
Helper file to create a redshift cluster and an IAM role.

**check_redshift_status.py**
Helper file to check the connection to the redshift cluster.

**connect_to_redshift.py**
Helper file to connect to the redshift cluster used in `create_tables.py` and `etl.py`.

**delete_redshift.py**
Helper file to detach the IAM role, delete the IAM role and delete the redshift cluster.

**sql_queries.py**
File to create the tables and insert data into them.

**create_tables.py**
File to call `sql_queries.py` to create the tables.

**etl**.py
Main file to call `sql_queries.py` to insert data into the tables created by `create_tables.py`.
The ETL pipeline consists of two steps:

1. Loading the data from S3 into the staging tables on redshift.
2. Inserting information from the staging tables in redshift into the final fact and dimension tables.

**README**.md

### Main Running Order
To complete this project the following steps must be done:
```sh
%run create_redshift.py
```
To create a redshift cluster on AWS.

After the creation of the cluster, the **(optional)** command:
```sh
%run check_redshift_status.py
```
can be run in order to varify that the cluster is running.

The next step is to create the necessary tables:
```sh
%run create_tables.py
```

and fill those tables with content:
```sh
%run etl.py
```

Finally, to delete the redshift cluster:
```sh
%run delete_redshift.py
```

### Database Schema

The following schema has been selected for the two staging tables.

**Staging Events Table**
* artist text
* auth text
* firstName text
* gender text 
* itemInSession integer
* lastName text 
* length double precision
* level text
* location text 
* method text
* page text 
* registration double precision
* sessionId integer
* song text
* status integer
* ts bigint
* userAgent text
* user_id text

**Staging Songs Table**
* artist_id text
* artist_latitude double precision
* artist_location text 
* artist_longitude double precision
* artist_name text 
* duration double precision
* num_songs integer
* song_id text 
* title text 
* year integer

With the above considerations, the following schema was selected for the fact, and dimension analytics tables;

**Fact Table**
**songplays** - distributed by key

* songplay_id int NOT NULL IDENTITY(0,1) 
* start_time timestamp NOT NULL REFERENCES time(start_time) 
* user_id int NOT NULL REFERENCES users(user_id) 
* level text 
* song_id text REFERENCES songs(song_id) 
* artist_id text REFERENCES artists(artist_id) 
* session_id int
* location text
* user_agent text

The PRIMARY KEY is set to `songplay_id` with the FOREIGN KEYS `start_time`, `user_id`, `song_id` and `artist_id`, referencing the tables `time`, `users`, `songs` and `artists` respectively.

The distribution by the key `song_id` is chosen because JOINS will be most certainly made with the `song_id`.
Furthermore, this table is SORTED by the `session_id` KEY since filtering on sessions might be useful for further analyses.

**Dimension Tables**
**users** - all distribution

* user_id int NOT NULL 
* first_name text NOT NULL 
* last_name text NOT NULL 
* gender text 
* level text NOT NULL

The PRIMARY KEY is set to `user_id`.

The all distribution is chosen because JOINS will be made with the `songplays` table and this table is most likely just slowly growing regarding the dimensions..

**songs** - distributed by key

* song_id text NOT NULL 
* title text
* artist_id text NOT NULL REFERENCES artists(artist_id) 
* year int 
* duration float

The PRIMARY KEY is set to `song_id` with the FOREIGN KEY `artist_id`, referencing the table `artists`
The distribution by the key `song_id` is chosen because JOINS will be most certainly made with the `song_id`.
Furthermore, this table is SORTED by the `year` KEY since filtering on sessions might be useful for further analyses.

**artists** - all distribution

* artist_id text NOT NULL
* name text NOT NULL 
* location text
* latitude float
* longitude float
* 
The PRIMARY KEY is set to `artist_id`.
The all distribution is chosen because this table is most likely just slowly growing regarding the dimensions.

**time** - auto distribution

* start_time timestamp
* hour int
* day int
* week int
* month int
* year int
* weekday text
* 
The PRIMARY KEY is set to `start_time`.
The auto distribution is chosen because there might be just few JOINS to be performed on this table.