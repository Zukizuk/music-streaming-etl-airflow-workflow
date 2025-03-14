from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import pandas as pd
from io import StringIO, BytesIO
from airflow.providers.postgres.hooks.postgres import PostgresHook
import redis
import json
import datetime

now = datetime.datetime.now()

EXPECTED_STREAM_COLUMNS = {"user_id", "track_id", "listen_time"}
EXPECTED_USER_COLUMNS = {"user_id", "user_name", "user_age", "user_country", "created_at"}
EXPECTED_SONG_COLUMNS = {"track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature", "track_genre"}
SONG_COLUMNS_TO_KEEP = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms","track_genre"]
s3_base_path = f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
REDIS_CLIENT = None
AWS_CLIENTS = {}
PG_CONNECTION = None

# Utility functions
def get_pg_connection():
    global PG_CONNECTION
    if PG_CONNECTION is None:
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
        PG_CONNECTION = pg_hook.get_conn()
    return PG_CONNECTION  # ✅ Reuse the existing connection

def get_aws_client(client_type: str):
    """Returns a cached AWS client if it exists, otherwise creates a new one."""
    if client_type not in AWS_CLIENTS:
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type=client_type)
        AWS_CLIENTS[client_type] = aws_hook.get_client_type()
    return AWS_CLIENTS[client_type]  # ✅ Reuse the existing client

def get_redis_client():
    global REDIS_CLIENT
    if REDIS_CLIENT is None:
        REDIS_CLIENT = redis.Redis(
            host='******',
            port=6379,
            password='******',
            ssl=True
        )
    return REDIS_CLIENT  # ✅ Reuse the same connection

def fetch_files_from_s3(bucket_name="etl-airflow-bucket-zuki", keys=None, prefix=None, all_files=False):
    """Fetch files from S3 based on keys, prefix, or all_files flag."""
    s3_client = get_aws_client("s3")
    
    # Get the list of keys
    file_keys = fetch_keys_from_s3(s3_client, bucket_name, keys, prefix, all_files)
    
    files = {}
    for key in file_keys:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        files[key] = response["Body"].read().decode("utf-8")  # Read and decode file content

    return files

def convert_files_to_dataframes(files):
    """Convert files to dataframes."""
    dataframes = {}
    for key, content in files.items():
        dataframes[key.split("/")[-1]] = pd.read_csv(StringIO(content))
    return dataframes
# Reading from sources functions
def fetch_keys_from_s3(s3_client, bucket_name, keys=None, prefix=None, all_files=False):
    """Determine and fetch keys from S3 based on provided parameters."""
    if keys and len(keys) > 0:
        print("Fetching files based on provided keys...")
        return keys
    
    elif prefix:
        print(f"Fetching files under prefix: {prefix}")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            print("No files found for the specified prefix.")
            return []
        return [obj["Key"] for obj in response["Contents"]]
    
    elif all_files:
        print("Fetching all files from the bucket...")
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" not in response:
            print("No files found in the bucket.")
            return []
        return [obj["Key"] for obj in response["Contents"]]
    
    else:
        print("No keys provided, no prefix specified and 'all' is False. Nothing to fetch.")
        return []

# def fetch_and_store_unique_values():
def process_s3_file(s3_client, bucket_name, key):
    """Process a single S3 file and return its dataframe."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))
        unique_users = df["user_id"].unique()
        unique_tracks = df["track_id"].unique()
        dict_key = key.split("/")[-1]  # Extract filename from key
        return dict_key, df.head(2), unique_users, unique_tracks # TODO: Change to df.head() for all data
    except pd.errors.EmptyDataError:
        print(f"Empty file: {key}")
        return None
    except Exception as e:
        print(f"Error processing file {key}: {e}")
        return None

def store_dataframes_in_redis(data_dict):
    """Store dataframes in Redis."""
    r = get_redis_client()
    stored_files = []
    
    if not data_dict:
        print("No data fetched from S3 or empty files found.")
        return []
    
    for key, value in data_dict.items():
        r.set(key, value.to_json(), nx=True)
        stored_files.append(key)
        print(f"{key} saved to Redis")
    
    return stored_files

def start_pipeline(keys: list = None, all: bool = False, prefix: str = "data/streams/"):
    """Fetch files from S3 bucket."""
    s3_client = get_aws_client("s3")
    bucket_name = "etl-airflow-bucket-zuki"

    try:
        # Get keys from S3
        keys = fetch_keys_from_s3(s3_client, bucket_name, keys, prefix, all)
        
        # Process each file
        data_dict = {}
        unique_val_dict = {
            "unique_users_ids": [],
            "unique_tracks_ids": []
        }
        
        for key in keys:
            dict_key, df, unique_users, unique_tracks = process_s3_file(s3_client, bucket_name, key)
            if dict_key and df is not None:
                data_dict[dict_key] = df
                unique_val_dict["unique_users_ids"].extend(unique_users.tolist())
                unique_val_dict["unique_tracks_ids"].extend(unique_tracks.tolist())
        
        # Store unique values in Redis
        r = get_redis_client()
        r.set("unique_stream_values", json.dumps(unique_val_dict), nx=True)
        print("Unique values saved to Redis. ✅")

        # Read user and song data from PostgreSQL
        read_user_data(unique_val_dict["unique_users_ids"])
        read_songs_data(unique_val_dict["unique_tracks_ids"])
        # Store in Redis
        return store_dataframes_in_redis(data_dict)
    except Exception as e:
        print(f"Failed to fetch files from S3: {e}")
        return {}

def read_user_data(user_ids: list):
    """Read user data from PostgreSQL"""
    connection = get_pg_connection()
    cursor = connection.cursor()

    table_name = "labs.users"


    try:
        query = f"SELECT * FROM {table_name} WHERE user_id IN {tuple(user_ids)}"
        df = pd.read_sql(query, connection)

        # Save to S3 as CSV for further processing
        s3_client = get_aws_client("s3")
        bucket_name = "etl-airflow-bucket-zuki"

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key="temp/users.csv", Body=csv_buffer.getvalue())
        print(f"✅ Data saved to S3: {bucket_name}/temp/users.csv")
    except Exception as e:
        print(f"Error reading user data: {e}")

def read_songs_data(songs_ids: list):
    """Read songs data from PostgreSQL"""
    connection = get_pg_connection()
    cursor = connection.cursor()
    try:
        # ✅ Ensure correct PostgreSQL table name
        table_name = "labs.songs"

        query = f"SELECT {', '.join(SONG_COLUMNS_TO_KEEP)} FROM {table_name} WHERE track_id IN {tuple(songs_ids)}"
        df = pd.read_sql(query, connection)

        # Save to S3 as CSV for further processing
        s3_client = get_aws_client("s3")
        bucket_name = "etl-airflow-bucket-zuki"

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key="temp/songs.csv", Body=csv_buffer.getvalue())
        print(f"✅ Data saved to S3: {bucket_name}/temp/songs.csv")
    except Exception as e:
        print(f"Error reading songs data: {e}")

# Validation functions
def validate_data(ti):
    """Validating data before EDA"""
    # This function perform validation before the start of the EDA process. Should any of the validation fail, the process should be terminated
    try:
        # Get keys from XCom
        keys = ti.xcom_pull(task_ids="read_s3_data_task")
        print(f"Keys from XCom: {keys}")
        if not keys:
            raise ValueError("No keys provided. Please provide keys to validate.")

        # Get stored files content from Redis and validate
        r = get_redis_client()
        dataframes = {}
        for key in keys:
            data = r.get(key)
            if not data:
                raise ValueError(f"Data for key '{key}' not found in Redis.")

            # Convert data to DataFrame
            df = pd.read_json(StringIO(data.decode("utf-8")))  # ✅ Decode bytes to string
            dataframes[key] = df

        # Validate columns
        for key, df in dataframes.items():
            if not set(df.columns) == EXPECTED_STREAM_COLUMNS:
                # Delete the key from Redis
                r.delete(key)
                raise ValueError(f"Columns in {key} are not as expected.")
            print(f"Validation passed for columns in {key}. ✅")
        

        print("Validation successful. Proceed to Transformation. ✅🎉")
        # return keys
        return [key for key in dataframes.keys()]
    except Exception as e:
        print(f"Validation failed: {e}")
        return False
    
def basic_transformations(df: pd.DataFrame, key: str):
    if "stream" in key:
        df["listen_time"] = pd.to_datetime(df["listen_time"])
        return df
    if "users" in key:
        df = df.dropna()
        df = df.drop_duplicates()
        df["created_at"] = pd.to_datetime(df["created_at"])
        return df
    if "songs" in key:
        df = df.dropna()
        df = df.drop_duplicates()
        return df


# EDA functions
def perform_transformations(ti):
    keys = ti.xcom_pull(task_ids="validate_data_task")
    try:
        r = get_redis_client()
        dataframes = {}
        for key in keys:
            data = r.get(key)
            df = pd.read_json(StringIO(data.decode("utf-8")))
            dataframes[key] = df
        # Get temp files from S3 for processing
        files = fetch_files_from_s3(keys=["temp/users.csv", "temp/songs.csv"])
        dataframes.update(convert_files_to_dataframes(files))
        # Perform transformations
        for key, df in dataframes.items():
            dataframes[key] = basic_transformations(df, key)
            print(f"Transformation successful for {key}. ✅")
        # TODO: Perform additional transformations like feature engineering 

        merged_df = merge_data_for_computation(dataframes)
        # Save merged data to S3 as Parquet for analysis
        s3_client = get_aws_client("s3")
        bucket_name = "etl-airflow-bucket-zuki"
        parquet_buffer = BytesIO()
        merged_df.to_parquet(parquet_buffer, index=False)  # Write Parquet data as binary
        parquet_buffer.seek(0)  # Reset buffer position

        s3_client.put_object(Bucket=bucket_name, Key=f"{s3_base_path}streams.parquet", Body=parquet_buffer.getvalue())
        print(f"✅ Merged data saved to S3: {bucket_name}/{s3_base_path}streams.parquet")
        # Save transformed data back to Redis as JSON to preserve data types
        # for key, df in dataframes.items():
        #     new_key = f"transformed_{key}"
        #     r.set(new_key, df.to_json(), nx=True)
        #     print(f"{new_key} saved to Redis. ✅")
        # return [f"transformed_{key}" for key in dataframes.keys()]
            
    except Exception as e:
        print(f"Transformations failed: {e}")
        return False

def merge_data_for_computation(dataframes):
    """Merge dataframes for further computation."""
    try:
        # Merge all stream files and save to stream_df
        stream_df = None  # Initialize before the loop

        for key, df in dataframes.items():
            if "stream" in key:
                if stream_df is None:
                    stream_df = df  
                else:
                    stream_df = pd.concat([stream_df, df], ignore_index=True)  # Append subsequent DataFrames

        # Merge dataframes
        if stream_df is None:
            raise ValueError("No stream data found.")
        users_df = dataframes["users.csv"]
        songs_df = dataframes["songs.csv"]
        print("Merging data...")
        merged_df = pd.merge(stream_df, songs_df, on='track_id', how='left')
        print("Songs merged successfully. ✅")
        merged_df = pd.merge(merged_df, users_df, on='user_id', how='left')
        print("Data merged successfully. ✅")
        return merged_df
    except Exception as e:
        print(f"Error merging data: {e}")
        return False

def compute_hourly_store_kpi_redshift():
    """Compute Hourly KPIs from S3 and Store in Redshift"""
    try:
        s3_client = get_aws_client("s3")  # ✅ Get S3 client
        # ✅ Define S3 path for current hour
        s3_path = f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/streams.parquet"

        # ✅ Fetch Data from S3
        response = s3_client.get_object(Bucket="etl-airflow-bucket-zuki", Key=s3_path)
        streams_df = pd.read_parquet(BytesIO(response["Body"].read()))

        # ✅ Compute Unique Listeners
        unique_listeners = streams_df["user_id"].nunique()

        # ✅ Compute Top Artists per Hour
        top_artists = (
            streams_df.groupby("artists")["track_id"]
            .count()
            .reset_index()
            .rename(columns={"track_id": "play_count"})
            .sort_values(by="play_count", ascending=False)
            .head(5)  # Get Top 5 Artists
        )

        # ✅ Compute Track Diversity Index (Unique Tracks / Total Plays)
        total_plays = len(streams_df)
        unique_tracks = streams_df["track_id"].nunique()
        track_diversity_index = unique_tracks / total_plays if total_plays > 0 else 0

        # ✅ Store in Redshift
        redshift_hook = PostgresHook(postgres_conn_id="redshift_conn_id")
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()


        create_tables(cursor)
        print("Tables created successfully. ✅")
        # ✅ Insert Unique Listeners KPI
        cursor.execute(
            "INSERT INTO hourly_kpis (timestamp, unique_listeners) VALUES (%s, %s)",
            (now, unique_listeners),
        )
        print("Unique Listeners KPI stored successfully. ✅")
        # ✅ Insert Top Artists KPI
        for _, row in top_artists.iterrows():
            cursor.execute(
                "INSERT INTO hourly_top_artists (timestamp, artist, play_count) VALUES (%s, %s, %s)",
                (now, row["artists"], row["play_count"]),
            )
        print("Top Artists KPI stored successfully. ✅")
        # ✅ Insert Track Diversity Index KPI
        cursor.execute(
            "INSERT INTO hourly_track_diversity (timestamp, diversity_index) VALUES (%s, %s)",
            (now, track_diversity_index),
        )
        print("Track Diversity Index KPI stored successfully. ✅")
        connection.commit()
        cursor.close()
        connection.close()

        print("✅ Hourly KPIs stored in Redshift successfully.")
    except Exception as e:
        print(f"Failed to compute and store hourly KPIs: {e}")

def create_tables(cursor):
            # ✅ Create Tables if they don't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS hourly_kpis (
            timestamp TIMESTAMP PRIMARY KEY,
            unique_listeners INT
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS hourly_top_artists (
            timestamp TIMESTAMP,
            artist VARCHAR(255),
            play_count INT
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS hourly_track_diversity (
            timestamp TIMESTAMP PRIMARY KEY,
            diversity_index FLOAT
            )
        """
        )