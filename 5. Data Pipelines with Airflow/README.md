## Project: Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

## Data Source 
* Log data: `s3://udacity-dend/log_data`
* Song data: `s3://udacity-dend/song_data`

## Schema for Song Play Analysis
### Song Plays table

- *Name:* `songplays`
- *Type:* Fact table

| Column | Description |
| ------ | ----------- |
| `songplay_id` | The main identification of the table | 
| `start_time` | The timestamp that this song play log happened |
| `user_id` | The user id that triggered this song play log. It cannot be null, as we don't have song play logs without being triggered by an user.  |
| `level` | The level of the user that triggered this song play log |
| `song_id` | The identification of the song that was played. It can be null.  |
| `artist_id` | The identification of the artist of the song that was played. |
| `session_id` | The session_id of the user on the app |
| `location` | The location where this song play log was triggered  |
| `user_agent` | The user agent of our app |

### Users table

- *Name:* `users`
- *Type:* Dimension table

| Column | Description |
| ------ | ----------- |
| `user_id` | The main identification of an user |
| `first_name` | First name of the user, can not be null. It is the basic information we have from the user |
| `last_name` | Last name of the user. |
| `gender` | The gender is stated with just one character `M` (male) or `F` (female). Otherwise it can be stated as `NULL` |
| `level` | The level stands for the user app plans (`premium` or `free`) |


### Songs table

- *Name:* `songs`
- *Type:* Dimension table

| Column | Description |
| ------ | ----------- |
| `song_id` | The main identification of a song | 
| `title` | The title of the song. It can not be null, as it is the basic information we have about a song. |
| `artist_id` | The artist id, it can not be null as we don't have songs without an artist, and this field also references the artists table. |
| `year` | The year that this song was made |
| `duration` | The duration of the song |


### Artists table

- *Name:* `artists`
- *Type:* Dimension table

| Column | Description |
| ------ | ----------- |
| `artist_id` | The main identification of an artist |
| `name` | The name of the artist |
| `location` | The location where the artist are from |
| `latitude` | The latitude of the location that the artist are from |
| `longitude` | The longitude of the location that the artist are from |

### Time table

- *Name:* `time`
- *Type:* Dimension table

| Column | Description |
| ------ | ----------- |
| `start_time` | The timestamp itself, serves as the main identification of this table |
| `hour` | The hour from the timestamp  |
| `day` | The day of the month from the timestamp |
| `week` | The week of the year from the timestamp |
| `month` | The month of the year from the timestamp |
| `year` | The year from the timestamp |
| `weekday` | The week day from the timestamp |

## Project files
- `etl.py` - reads data from S3, processes that data using Spark, and writes them back to S3
- `dl.cfg` - contains your AWS credentials
- `README.md` - provide discussion on process and decisions for this pipeline
- `Notebok.ipynb` - the notebook contains the preparatory steps for etl.py

## Project Strucute
* README: Current file, holds instructions and documentation of the project
* dags/dag.py: Directed Acyclic Graph definition with imports, tasks and task dependencies
* dags/subdag.py: SubDag containing loading of Dimensional tables tasks
* plugins/helpers/sql_queries.py: Contains Insert SQL statements
* plugins/operators/stage_redshift.py: Operator that copies data from S3 buckets into redshift staging tables
* plugins/operators/load_dimension.py: Operator that loads data from redshift staging tables into dimensional tables
* plugins/operators/load_fact.py: Operator that loads data from redshift staging tables into fact table
* plugins/operators/data_quality.py: Operator that validates data quality in redshift tables

## Sparkify DAG
DAG parameters:

* The DAG does not have dependencies on past runs
* DAG has schedule interval set to hourly
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Email are not sent on retry

DAG contains default_args dict bind to the DAG, with the following keys:
   
    * Owner
    * Depends_on_past
    * Start_date
    * Retries
    * Retry_delay
    * Catchup

## Operators
Operators create necessary tables, stage the data, transform the data, and run checks on data quality.

Connections and Hooks are configured using Airflow's built-in functionalities.

All of the operators and task run SQL statements against the Redshift database. 
