## Project Description
In this project, I applied what I've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I loaded data from S3, processed the data into analytics tables using Spark, and loaded them back into S3. Then deployed this Spark process on a cluster using AWS.

## Project Datasets
- Song data:  `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

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