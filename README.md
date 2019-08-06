# PROJECT-4: Udacity DEND - Data Lake

## Quick start

Fill in AWS acces key (KEY) and secret (SECRET) in dl.cfg file.

Sample data is in data folder. To run the script to use that data, do the following:

* Create an AWS S3 bucket.
* Edit the file dl.cfg: add S3 bucket created in the above step.
* In the S3 location, copy **log_data** and **song_data** folders.
* In the S3 location create an empty folder called **output_data**.

After installing python3 + Apache Spark (pyspark) libraries and dependencies, run the following command:

* `python etl.py` (to process all the input JSON data into Spark parquet files)

* Script can be run locally as well by uncommenting/commenting rows in etl.py main() function.
---

## Overview

This project handles data of a fictional music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets (or in your local file path) and contains two parts:

* **Songs Data - s3://udacity-dend/song_data**: Static data about artists and songs
  Example:
  `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

* **Log Data - s3://udacity-dend/log_data**: Event logs of all the users using the service containing details such as what song listened to, which client was used etc. 

  `{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}`
 
Purpose of the project is to build an ETL (Extract, Transform and Load) pipeline to extract data from JSON files stored in AWS S3 location using Apache Spark, and write data back into the AWS S3 location in parquet format. Technologies used - PySpark, Python, AWS S3.

NOTE: ETL script has been tested with limited input data (stored in S3) due to very slow S3 read and write procedure.

---

## Database Overview

The fictional music streaming company's database, called sparkifydb has a star schema design. Star schema refers to having one Fact table with quantifiable business data and supporting Dimensions table data.


### Fact Table

* **fact_songplay**: songs play data together with user, artist, and song info (songplay_id - auto-increment id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables

* **dim_user**: user info (columns: user_id, first_name, last_name, gender, level)
* **dim_song**: song info (columns: song_id, title, artist_id, year, duration)
* **dim_artist**: artist info (columns: artist_id, name, location, latitude, longitude)
* **dim_time**: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

---

## How to use?

**Project has one script:**

* **etl.py**: This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into a star schema database.

### Prerequisites

Python3 is recommended as the environment. The most convenient way to install python is by using Anaconda (https://www.anaconda.com/distribution/).


### Run etl.py

Type to command line:

`python etl.py`

