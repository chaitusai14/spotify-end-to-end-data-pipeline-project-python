# Spotify End to End Data Pipeline using Python 
This project demonstrates an ETL (Extract, Transform, Load) pipeline using AWS services to extract data from the Spotify API, transform it, and load it into a data lake for further analysis. The pipeline is designed to automate the daily extraction and transformation of Spotify data and make it available for querying with Amazon Athena.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [AWS Services Used](#aws-services-used)
3. [Data Schema](#data-schema)
4. [Python Lambda Functions](#python-lambda-functions)
5. [Transformation Logic](#transformation-logic)
6. [Setup Instructions](#setup-instructions)
7. [Usage](#usage)

## Architecture Overview

![Architecture Diagram](https://github.com/chaitusai14/spotify-end-to-end-data-pipeline-project-python/blob/main/Data_Pipeline_Architecture.png)

The ETL pipeline follows a standard architecture for extracting, transforming, and loading data from the Spotify API into an AWS data lake:

1. **Extract**: Data is extracted from the Spotify API using AWS Lambda and stored in Amazon S3 as raw data.
2. **Transform**: The extracted data is processed and transformed using another AWS Lambda function, and the transformed data is saved back to a different S3 bucket.
3. **Load**: AWS Glue is used to crawl the transformed data in S3 and infer its schema, making it available in the AWS Glue Data Catalog. Finally, the data can be queried using Amazon Athena for analytics purposes.

## AWS Services Used

- **Amazon S3**: Used for storing both raw and transformed data.
- **AWS Lambda**: Two Lambda functions are used in this project:
  - `spotify_api_data_extract`: Extracts data from the Spotify API.
  - `spotify_transformation_load`: Transforms the extracted data and stores it in S3.
- **Amazon CloudWatch**: Used to schedule and trigger the Lambda functions.
- **AWS Glue**: Used to crawl the data in S3, infer its schema, and store the metadata in the Glue Data Catalog.
- **Amazon Athena**: Provides a serverless querying capability to analyze the transformed data using SQL.

## Data Schema

The data schema for this ETL pipeline is inferred by AWS Glue from the transformed data stored in Amazon S3. The following outlines the main data structures and their attributes that are extracted from the Spotify API:

1. **Tracks**:
   - `track_id` (String): Unique identifier for the track.
   - `track_name` (String): Name of the track.
   - `album_id` (String): Identifier of the album the track belongs to.
   - `artist_id` (String): Identifier of the primary artist for the track.
   - `duration_ms` (Integer): Duration of the track in milliseconds.
   - `popularity` (Integer): Popularity score of the track (0-100).
   - `explicit` (Boolean): Indicates whether the track has explicit content.
   - `release_date` (Date): Release date of the track.

2. **Artists**:
   - `artist_id` (String): Unique identifier for the artist.
   - `artist_name` (String): Name of the artist.
   - `genres` (Array of Strings): List of genres associated with the artist.
   - `followers` (Integer): Number of followers on Spotify.
   - `popularity` (Integer): Popularity score of the artist (0-100).

3. **Albums**:
   - `album_id` (String): Unique identifier for the album.
   - `album_name` (String): Name of the album.
   - `release_date` (Date): Release date of the album.
   - `total_tracks` (Integer): Total number of tracks in the album.
   - `album_type` (String): Type of the album (e.g., single, album, compilation).

## Python Lambda Functions

### 1. `spotify_api_data_extract.py`

This Lambda function handles the extraction of data from the Spotify API. It is triggered daily by Amazon CloudWatch. The extracted data is stored in a raw format in an S3 bucket.

#### Key Functionalities:
- Connects to the Spotify API using OAuth.
- Fetches data such as tracks, artists, albums, and playlists.
- Saves the raw JSON data to an S3 bucket.

### 2. `spotify_transformation_load.py`

This Lambda function is responsible for transforming the raw data extracted from the Spotify API into a format suitable for analysis. It is triggered whenever a new object is added to the S3 bucket (event-driven).

#### Key Functionalities:
- Reads raw JSON data from the S3 bucket.
- Applies necessary data transformations (e.g., data cleaning, filtering, and structuring).
- Writes the transformed data back to another S3 bucket for further processing.

## Transformation Logic

The `spotify_transformation_load.py` Lambda function applies the following transformation logic to the raw data:

1. **Data Cleaning**:
   - Remove any duplicate entries based on unique identifiers (e.g., `track_id`, `artist_id`).
   - Filter out tracks or albums that are missing critical metadata (e.g., no `track_name` or `album_id`).

2. **Data Structuring**:
   - Flatten nested JSON structures into a tabular format suitable for analysis.
   - Convert data types to ensure consistency (e.g., converting `release_date` from string to date format).

3. **Data Enrichment**:
   - Add calculated fields, such as `track_length_minutes` derived from `duration_ms`.
   - Join related data, such as adding artist names to tracks using `artist_id`.

4. **Data Aggregation**:
   - Aggregate data to produce summary statistics, such as the number of tracks per album or the average popularity score per artist.

5. **Data Formatting**:
   - Format the transformed data as Parquet or CSV files, optimized for querying with Amazon Athena.
   - Write the formatted data to the designated S3 bucket for transformed data.

## Setup Instructions

### Prerequisites

- AWS Account
- AWS CLI configured with appropriate IAM permissions
- Python 3.x installed locally
- Spotify Developer Account and API credentials

### Steps

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/chaitusai14/spotify-end-to-end-data-pipeline-project-python.git
    cd spotify-data-pipeline
    ```

2. **Set Up S3 Buckets**:
   - Create two S3 buckets: one for raw data and one for transformed data.
   
3. **Deploy Lambda Functions**:
   - Package the Lambda functions (`spotify_api_data_extract_lamda_function.py` and `spotify_transformation_load_lambda_function.py`).
   - Deploy them using the AWS CLI or AWS Management Console. Ensure each function has the appropriate IAM role with permissions to access S3 and CloudWatch.

4. **Configure CloudWatch Events**:
   - Set up a CloudWatch rule to trigger the `spotify_api_data_extract` Lambda function daily.
   - Configure an S3 event to trigger the `spotify_transformation_load` Lambda function on object creation.

5. **Set Up AWS Glue**:
   - Create an AWS Glue Crawler to crawl the transformed data S3 bucket and populate the Glue Data Catalog.

6. **Run the Pipeline**:
   - Once all components are set up, the pipeline will run automatically according to the scheduled and event-based triggers.

## Usage

After setting up the pipeline, the data extraction and transformation processes will run automatically based on the triggers. The extracted and transformed data will be available in the specified S3 buckets. You can use Amazon Athena to query the data directly from the AWS Glue Data Catalog.

To query the data:
1. Open the Amazon Athena console.
2. Select the database created by the AWS Glue Crawler.
3. Use standard SQL queries to analyze the transformed data.



