# Spotify Data Pipeline using AWS, Snowflake, and Airflow

This project demonstrates a robust data pipeline designed to fetch data from Spotify, process it using AWS services, and store the transformed data in Snowflake. The pipeline can be implemented with or without the orchestration capabilities of Apache Airflow.

## Project Overview

The Spotify Data Pipeline project is designed to automate the extraction, transformation, and loading (ETL) of data from Spotify. The data is first fetched from Spotify's API, then processed and transformed using AWS services like S3 and Glue, and finally stored in Snowflake for further analysis. The pipeline ensures efficient and scalable data handling, suitable for large datasets and complex transformations.

## Implementation

### Without Airflow

This section outlines the steps to implement the pipeline manually using scripts and AWS services.

#### 1. Fetch Data from Spotify

- **Script to Fetch Data**: Write a script to fetch data from Spotify and upload it to an S3 bucket.
- **Environment Variables**: Create environment variables for `client_id` and `client_secret` from the Spotify API.
- **Spotify Client Initialization**: Initialize the Spotify client using Spotify Manager.
- **Retrieve Playlists**: Use the client to retrieve playlists and other relevant data.

#### 2. Connection to AWS

- **AWS Connection**: Use `boto3` to create a client for S3.
- **File Naming**: Create a filename with the pattern `spotify_raw_<datetime>.json`.
- **IAM Permissions**: Ensure the IAM role has permissions to access S3 and Glue.
- **Upload to S3**: Upload the Spotify data to the S3 bucket, specifically to the `to_processed` folder.
- **Glue Job**: After uploading, create and start a Glue job. Check its status periodically.

#### 3. Glue Job

- **IAM Role for Glue**: Create an IAM role with permissions for S3 full access, Glue service role, and Glue notebook role.
- **Glue Development**: Create a Glue notebook for development purposes.
- **Glue Context**: Use Glue context, a modified version of SparkContext, for processing.
- **Transformation Logic**: Write transformation logic to extract data from the `to_processed` folder in S3, convert it to a DataFrame, and store it in the `transformed_data` folder in S3 in CSV format.

#### 4. Snowflake Integration

- **External Role**: Create an external role in IAM for Snowflake to access AWS services.
- **S3 Access**: Provide full access to S3.
- **Storage Connection**: Create a storage connection in Snowflake to the S3 bucket.
- **Trust Policy**: Update the IAM trust policy with the ARN and External ID from Snowflake.
- **File Format**: Create a file format in Snowflake for CSV files.
- **Stage Creation**: Create a stage in Snowflake with storage connection and file format.
- **Schema and Pipes**: Create a schema for Snowpipes, then create a pipe to copy data from the S3 stage to Snowflake tables.
- **Notification Channel**: Set up an SQS notification channel for the S3 bucket. Configure S3 event notifications to trigger on data uploads, sending notifications to the SQS queue.

### With Airflow

This section outlines the steps to implement the pipeline using Apache Airflow for orchestration.

#### 1. Airflow Setup

- **External Packages**: Add required external packages for Airflow in the Docker configuration.

    ```yaml
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- spotipy apache-airflow-providers-amazon apache-airflow-providers-google}
    ```

#### 2. Airflow DAG

1. **Fetch Data from Spotify**: Create a task to fetch data from Spotify.
2. **Store Data in S3**: Use the S3 create object operator to store data in S3.
3. **Read Data from S3**: Create a task to read data from S3.
4. **Process Data**:
    - Process albums and store the transformed data in S3.
    - Process artists and store the transformed data in S3.
    - Process songs and store the transformed data in S3.
5. **Move Processed Data**: Move data from `to_processed` to `processed` folder in S3.

#### 3. Lambda and Glue Integration

- **Lambda Function**: Create a Lambda function to fetch and store data into S3.
- **DAG Trigger**: Create an Airflow DAG to trigger the Lambda function.
- **Data Confirmation**: Check and confirm the data is uploaded to S3.
- **Trigger Glue Job**: Trigger a Glue job from Airflow to process the data.

## Conclusion

This project showcases a comprehensive approach to building a data pipeline using AWS, Snowflake, and Airflow. By following the steps outlined above, you can automate the ETL process for Spotify data, ensuring efficient data processing and storage for further analysis. Whether you choose to implement the pipeline manually or with Airflow, this project provides a solid foundation for handling complex data workflows.

## Getting Started

To get started with this project, follow these steps:

1. **Clone the repository**:

    ```bash
    git clone https://github.com/yourusername/Spotify-Data-Pipeline-using-AWS-Snowflake-and-Airflow.git
    cd Spotify-Data-Pipeline-using-AWS-Snowflake-and-Airflow
    ```

2. **Install required packages**:

    ```bash
    pip install -r requirements.txt
    ```

3. **Set up environment variables** for Spotify API credentials and AWS access:

    ```bash
    export SPOTIPY_CLIENT_ID='your_spotify_client_id'
    export SPOTIPY_CLIENT_SECRET='your_spotify_client_secret'
    export AWS_ACCESS_KEY_ID='your_aws_access_key'
    export AWS_SECRET_ACCESS_KEY='your_aws_secret_key'
    ```

4. **Run the data fetch script** to fetch data from Spotify and upload it to S3:

    ```bash
    python fetch_spotify_data.py
    ```

5. **Set up Airflow** (if using Airflow for orchestration):

    - Modify the `docker-compose.yml` file to include the required packages.
    - Initialize Airflow:

        ```bash
        docker-compose up airflow-init
        docker-compose up
        ```

    - Create and activate the Airflow DAG for the pipeline.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

