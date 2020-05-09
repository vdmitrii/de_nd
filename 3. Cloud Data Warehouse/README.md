## Project Description
In this project, I'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

## Project Datasets
- Song data:  `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`
- Log data json path: `s3://udacity-dend/log_json_path.json`

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
- `create_table.py` - create fact and dimension tables for the star schema in Redshift.
- `etl.py` - load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
- `sql_queries.py` - define SQL statements, which will be imported into the two other files above.
- `README.md` - provide discussion on process and decisions for this ETL pipeline.
- `Notebook.ipynb` - notebook include code for creating and running cluster, querring and deleting cluster

### Steps on this project

1. Create Table Schemas
- Design schemas for fact and dimension tables
- Write a SQL CREATE statement for each of these tables in sql_queries.py
- Complete the logic in create_tables.py to connect to the database and create these tables
- Write SQL DROP statements to drop tables in the beginning of - create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
- Launch a redshift cluster and create an IAM role that has read access to S3.
- Add redshift database and IAM role info to dwh.cfg.
- Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

2. Build ETL Pipeline
- Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
- Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
- Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.
- Delete your redshift cluster when finished.

3. Document Process
