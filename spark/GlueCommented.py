import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define the S3 path where raw data is stored
s3_path = "s3://spotify-etl-project-shivasai/raw_data/to_processed/"

# Import necessary functions from PySpark and other modules
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame

# Create a DynamicFrame from the JSON files stored in the specified S3 path
source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json"
)

# Convert the DynamicFrame to a Spark DataFrame
spotify_df = source_dyf.toDF()

# Function to process album data from the DataFrame
def process_album(df):
    # Explode the items array to create a row for each track's album
    df = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("album_url")
    ).dropDuplicates(['album_id'])  # Remove duplicate albums
    return df

# Function to process artist data from the DataFrame
def process_artist(df):
    # Explode the items array
    df_items_exploded = df.select(explode(col("items")).alias("item"))
    
    # Explode the artists array within each item
    df_artists_exploded = df_items_exploded.select(explode(col("item.track.artists")).alias("artist"))
    
    # Select and rename the necessary columns for artists
    df_artists = df_artists_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("artist_url")
    ).dropDuplicates(['artist_id'])  # Remove duplicate artists
    
    return df_artists

# Function to process song data from the DataFrame
def process_songs(df):
    # Explode the items array to create a row for each song
    df_exploded = df.select(explode(col("items")).alias("item"))
    
    # Select and rename the necessary columns for songs
    df_songs = df_exploded.select(
        col("item.track.id").alias("song_id"),
        col("item.track.name").alias("song_name"),
        col("item.track.duration_ms").alias("duration_ms"),
        col("item.track.external_urls.spotify").alias("url"),
        col("item.track.popularity").alias("popularity"),
        col("item.added_at").alias("song_added"),
        col("item.track.album.id").alias("album_id"),
        col("item.track.artists")[0]["id"].alias("artist_id")
    ).dropDuplicates(['song_id'])  # Remove duplicate songs
    
    # Convert string dates in 'song_added' to actual date types
    df_songs = df_songs.withColumn("song_added", to_date(col("song_added")))
    
    return df_songs

# Process the DataFrame to get album, artist, and song data
album_df = process_album(spotify_df)
artist_df = process_artist(spotify_df)
song_df = process_songs(spotify_df)

# Show the top 5 rows of each DataFrame for verification
album_df.show(5)
artist_df.show(5)
song_df.show(5)

# Function to write a DataFrame to S3 in a specified format
def write_to_s3(df, path_suffix, format_type="csv"):
    # Convert the Spark DataFrame to a DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    # Write the DynamicFrame to S3 in the specified format
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": f"s3://spotify-etl-project-shivasai/transformed_data/{path_suffix}/"},
        format=format_type
    )

# Write the processed DataFrames to S3
write_to_s3(album_df, f"albums_data/album_transformed_{datetime.now().strftime('%Y-%m-%d')}", "csv")
write_to_s3(artist_df, f"artists_data/artist_transformed_{datetime.now().strftime('%Y-%m-%d')}", "csv")
write_to_s3(song_df, f"songs_data/songs_transformed_{datetime.now().strftime('%Y-%m-%d')}", "csv")

# Function to list S3 objects with a specific prefix
def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.json')]
    return keys

# Define the bucket name and prefix for raw data
bucket_name = "spotify-etl-project-shivasai"
prefix = "raw_data/to_processed/"

# List the S3 objects with the specified prefix
spotify_keys = list_s3_objects(bucket_name, prefix)

# Function to move and delete files in S3
def move_and_delete_files(spotify_keys, Bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        # Define the destination key
        destination_key = 'raw_data/processed/' + key.split("/")[-1]
        
        # Copy the file to the new location
        s3_resource.meta.client.copy(copy_source, Bucket, destination_key)
        
        # Delete the original file
        s3_resource.Object(Bucket, key).delete()

# Move and delete the processed files in S3
move_and_delete_files(spotify_keys, bucket_name)

# Commit the Glue job
job.commit()
