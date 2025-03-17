from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import pandas as pd
from io import StringIO, BytesIO
from airflow.providers.postgres.hooks.postgres import PostgresHook
import redis
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import json
import datetime

now = datetime.datetime.now()
EXPECTED_STREAM_COLUMNS = {"user_id", "track_id", "listen_time"}
SONG_COLUMNS_TO_KEEP = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms","track_genre"]
s3_base_path = f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
REDIS_CLIENT = None
AWS_CLIENTS = {}
PG_CONNECTION = None
bucket_name = "etl-airflow-bucket-zuki"
source = "data/streams/"

# * Utility functions
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
            host='*****',
            port=6379,
            password='*****',
            ssl=True
        )
    return REDIS_CLIENT  # ✅ Reuse the same connection

def store_key_cleanup(key):
    """Store keys for clean up in the end."""
    try:
        r = get_redis_client()
        r.sadd("keys_to_clean", key)
    except Exception as e:
        print(f"Failed to store key for cleanup: {e}")

def fetch_files_from_s3(keys=None, prefix=None, all_files=False):
    """Fetch files from S3 based on keys, prefix, or all_files flag."""
    s3_client = get_aws_client("s3")
    
    # Get the list of keys
    file_keys = fetch_keys_from_s3(s3_client, bucket_name, keys, prefix, all_files)
    
    files = {}
    for key in file_keys:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)

        # ✅ Detect file format (Assuming all processed files are in Parquet)
        if key.endswith(".parquet"):
            files[key] = pd.read_parquet(BytesIO(response["Body"].read()))  # ✅ Read as Parquet
        else:
            files[key] = response["Body"].read().decode("utf-8")  # ✅ Read as text (for CSV/JSON)

    return files

def clean_redis_data(keys: list=[]):
    """Clean up Redis data after processing."""
    r = get_redis_client()
    for key in keys:
        r.delete(key)
    print("✅ Redis data cleaned up successfully.")

def clean_s3_files(keys: list=[]):
    """Clean up S3 files after processing."""
    try:
        s3_client = get_aws_client("s3")
        for key in keys:
            s3_client.delete_object(Bucket=bucket_name, Key=key)
        print("✅ S3 files cleaned up successfully.")
        return True
    except Exception as e:
        print(f"Failed to clean up S3 files: {e}")
        return False

def drop_redshift_tables(tables: list=[]):
    redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_conn_id')
    for table in tables:
        redshift_hook.run(f"DROP TABLE IF EXISTS {table};")
    print("✅ Redshift tables dropped successfully.")

def archive_processed_files():
    """Archive processed files in S3 by moving to a different folder."""
    try:
        s3_client = get_aws_client("s3")
        # ✅ Define S3 path for archive
        s3_archive_path = f"archive/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        
        # Fetch Data from S3
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source)
        if "Contents" in response:
            keys = [obj["Key"] for obj in response["Contents"]]
            for key in keys:
                new_key = key.replace(source, s3_archive_path)
                s3_client.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{key}", Key=new_key)
                s3_client.delete_object(Bucket=bucket_name, Key=key) 
            print(f"✅ Processed files archived to S3: {bucket_name}/{s3_archive_path}")
            return True
    except Exception as e:
        print(f"Failed to archive processed files: {e}")
        return False

def convert_files_to_dataframes(files):
    """Convert files to dataframes."""
    dataframes = {}
    for key, content in files.items():
        store_key_cleanup(key)
        dataframes[key.split("/")[-1]] = pd.read_csv(StringIO(content))
    return dataframes
# * Reading from sources functions

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


def process_s3_file(s3_client, bucket_name, key):
    """Process a single S3 file and return its dataframe."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))
        unique_users = df["user_id"].unique()
        unique_tracks = df["track_id"].unique()
        # unique users and tracks length
        dict_key = key.split("/")[-1]  # Extract filename from key
        return dict_key, df, unique_users, unique_tracks # TODO: Change to df.head() for all data
    except pd.errors.EmptyDataError:
        print(f"Empty file: {key}")
        return None
    except Exception as e:
        print(f"Error processing file {key}: {e}")
        return None


def store_dataframes_in_redis(ti):
    """Store dataframes in Redis."""
    r = get_redis_client()
    stored_files = []
    data_dict = ti.xcom_pull(key="data_dict")
    
    if not data_dict:
        print("No data fetched from S3 or empty files found.")
        return []
    
    for key, value in data_dict.items():
        r.set(key, value.to_json())
        stored_files.append(key)
        print(f"{key} saved to Redis")
    return stored_files


def extraction_streams_from_s3(prefix: str = source, **kwargs):
    """Fetch files from S3 bucket."""
    s3_client = get_aws_client("s3")
    ti = kwargs["ti"]
    try:
        # Get keys from S3
        keys = fetch_keys_from_s3(s3_client, bucket_name=bucket_name, prefix=prefix)
        
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
        store_key_cleanup("unique_stream_values")
        print("Unique values saved to Redis. ✅")

        ti.xcom_push(key="unique_users_ids", value=unique_val_dict["unique_users_ids"])
        ti.xcom_push(key="unique_tracks_ids", value=unique_val_dict["unique_tracks_ids"])
        ti.xcom_push(key="data_dict", value=data_dict)
        return "Streams data fetched from S3."
    except Exception as e:
        print(f"Failed to fetch files from S3: {e}")
        return {}


def extract_users_data(ti):
    """Read user data from PostgreSQL"""
    connection = get_pg_connection()
    cursor = connection.cursor()
    user_ids = ti.xcom_pull(key="unique_users_ids")

    table_name = "labs.users"
    print(f"Shape of user_ids: {len(user_ids)}")

    try:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall())
        df = df[df['user_id'].isin(user_ids)]

        # Save to S3 as CSV for further processing
        s3_client = get_aws_client("s3")
        

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key="temp/users.csv", Body=csv_buffer.getvalue())
        store_key_cleanup("temp/users.csv")
        print(f"✅ Data saved to S3: {bucket_name}/temp/users.csv")
    except Exception as e:
        print(f"Error reading user data: {e}")


def extract_songs_data(ti):
    """Read songs data from PostgreSQL"""
    connection = get_pg_connection()
    cursor = connection.cursor()
    songs_ids = ti.xcom_pull(key="unique_tracks_ids")
    try:
        # ✅ Ensure correct PostgreSQL table name
        table_name = "labs.songs"
        print(f"Shape of songs_ids: {len(songs_ids)}")

        query = f"SELECT {', '.join(SONG_COLUMNS_TO_KEEP)} FROM {table_name}"
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=SONG_COLUMNS_TO_KEEP)
        df = df[df["track_id"].isin(songs_ids)]

        # Save to S3 as CSV for further processing
        s3_client = get_aws_client("s3")
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key="temp/songs.csv", Body=csv_buffer.getvalue())
        store_key_cleanup("temp/songs.csv")
        print(f"✅ Data saved to S3: {bucket_name}/temp/songs.csv")
    except Exception as e:
        print(f"Error reading songs data: {e}")


# * Validation functions
def validate_data(ti):
    """Validating data before EDA"""
    # This function perform validation before the start of the EDA process. Should any of the validation fail, the process should be terminated
    try:
        # Get keys from XCom
        keys = ti.xcom_pull(task_ids="data_extraction.store_dataframes_in_redis")
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
        
        for key in keys:
            store_key_cleanup(key)
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


# * EDA functions
def perform_transformations(ti):
    keys = ti.xcom_pull(task_ids="validate_data_task")
    try:
        r = get_redis_client()
        dataframes = {}
        # Get data from Redis
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

        streams_df = merge_data_for_computation(dataframes)
        # Save merged data to S3 as Parquet for analysis
        s3_client = get_aws_client("s3")
        parquet_buffer = BytesIO()
        streams_df.to_parquet(parquet_buffer, index=False)  # Write Parquet data as binary
        parquet_buffer.seek(0)  # Reset buffer position

        s3_client.put_object(Bucket=bucket_name, Key=f"{s3_base_path}streams.parquet", Body=parquet_buffer.getvalue())
        print(f"✅ Merged data saved to S3: {bucket_name}/{s3_base_path}streams.parquet")
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
        print("Stream data merged successfully. ✅")
        print(f"Shape of merged stream data: {stream_df.shape}")
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


def compute_genre_kpis(ti):
    s3_client = get_aws_client("s3")
    s3_path = f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/streams.parquet"

    # ✅ Fetch Data from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
    streams_df = pd.read_parquet(BytesIO(response["Body"].read()))
    # ================================
    # Compute Genre-Level KPIs (by day)
    # ================================

    # Extract date from listen_time for daily genre KPIs
    streams_df['date'] = pd.to_datetime(streams_df['listen_time']).dt.date
    genre_kpis = streams_df.groupby(['date', 'track_genre']).agg(
        listen_count=('track_id', 'count'),
        avg_track_duration=('duration_ms', 'mean'),
        avg_popularity=('popularity', 'mean')
    ).reset_index()

    # Calculate popularity index (simplified calculation)
    genre_kpis['popularity_index'] = genre_kpis['listen_count'] * genre_kpis['avg_popularity'] / 100

    # Find most popular track per genre per day
    popular_tracks = streams_df.groupby(['date', 'track_genre', 'track_name']).size().reset_index(name='plays')
    most_popular_tracks = popular_tracks.sort_values(['date', 'track_genre', 'plays'], ascending=[True, True, False]).drop_duplicates(subset=['date', 'track_genre'])

    # Merge with genre_kpis
    genre_kpis = pd.merge(genre_kpis, most_popular_tracks[['date', 'track_genre', 'track_name']], 
                        on=['date', 'track_genre'], how='left')
    genre_kpis = genre_kpis.rename(columns={'track_name': 'most_popular_track'})

    ti.xcom_push(key="genre_kpis", value=genre_kpis)
    print("✅ Genre KPIs computed and saved to XCom.")
    return True


def compute_hourly_kpis(ti):
    s3_client = get_aws_client("s3")
    s3_path = f"processed/year={now.year}/month={now.month:02d}/day={now.day:02d}/streams.parquet"

    # ✅ Fetch Data from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
    streams_df = pd.read_parquet(BytesIO(response["Body"].read()))
    # ================================
    # Compute Hourly KPIs
    # ================================
            # Extract hour from listen_time for hourly KPIs
    streams_df['hour'] = pd.to_datetime(streams_df['listen_time']).dt.hour
    
    # Extract date from listen_time for daily genre KPIs
    streams_df['date'] = pd.to_datetime(streams_df['listen_time']).dt.date
    hourly_kpis = streams_df.groupby(['date', 'hour']).agg(
        unique_listeners=('user_id', 'nunique'),
        total_plays=('track_id', 'count'),
        unique_tracks=('track_id', 'nunique')
    ).reset_index()

    # Calculate track diversity index
    hourly_kpis['track_diversity_index'] = hourly_kpis['unique_tracks'] / hourly_kpis['total_plays']

    # Find top artist per hour per date
    hourly_artists = streams_df.groupby(['date', 'hour', 'artists']).size().reset_index(name='play_count')
    top_artists = hourly_artists.sort_values(['date', 'hour', 'play_count'], ascending=[True, True, False]).drop_duplicates(subset=['date', 'hour'])

    # Merge with hourly_kpis
    hourly_kpis = pd.merge(hourly_kpis, top_artists[['date', 'hour', 'artists']], on=['date', 'hour'], how='left')
    hourly_kpis = hourly_kpis.rename(columns={'artists': 'top_artist'})

    ti.xcom_push(key="hourly_kpis", value=hourly_kpis)
    print("✅ Hourly KPIs computed and saved to XCom.")
    return True


# * Clean Task Functions
def perform_archive_clean_up():
    """Perform clean up of processed files in S3."""
    try:
        r = get_redis_client()
        keys = r.smembers("keys_to_clean")  # Returns a set of bytes

        # Decode keys to strings
        keys = {key.decode("utf-8") for key in keys}

        if archive_processed_files():
            clean_s3_files([key for key in keys if key.startswith("temp/")])
            clean_redis_data(keys)
            
        r.delete("keys_to_clean")
        print("✅ Clean up completed successfully.")
        return True
    except Exception as e:
        print(f"Failed to perform clean up: {e}")
        return False

# * Cleanup on failure task
def cleanup_on_failure():
    """Cleanup on failure."""
    r = get_redis_client()
    keys = r.smembers("keys_to_clean")  # Returns a set of bytes
    if keys:
        tables = ['genre_kpis', 'hourly_kpis']
        # Decode keys to strings
        keys = {key.decode("utf-8") for key in keys}
        clean_s3_files([key for key in keys if key.startswith("temp/")] + [f"{s3_base_path}streams.parquet"])
        clean_redis_data(keys)
        drop_redshift_tables(tables)

        r.delete("keys_to_clean")
        print("✅ Clean up on failure completed successfully.")
        return "✅ Clean up on failure completed successfully."
    else:
        print("✅ Nothing to clean up.")
        return "✅ Nothing to clean up."


def create_redshift_tables():
    """Create Redshift tables if they don't exist."""
    try:
        redshift_hook = RedshiftSQLHook("redshift_conn_id")
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()

        # Create genre KPIs table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS genre_kpis (
                date DATE,
                genre VARCHAR(255),
                listen_count INTEGER,
                avg_track_duration FLOAT,
                avg_popularity FLOAT,
                popularity_index FLOAT,
                most_popular_track VARCHAR(255),
                timestamp TIMESTAMP,
                PRIMARY KEY (date, genre)
            )
        """)

        # Create hourly KPIs table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_kpis (
                listen_date DATE,
                hour INTEGER,
                unique_listeners INTEGER,
                total_plays INTEGER,
                unique_tracks INTEGER,
                track_diversity_index FLOAT,
                top_artist VARCHAR(255),
                timestamp TIMESTAMP,
                PRIMARY KEY (hour, timestamp)
            )
        """)

        connection.commit()
        cursor.close()
        connection.close()
        print("✅ Redshift tables created successfully.")
        return True
    except Exception as e:
        print(f"Failed to create Redshift tables: {e}")
        cursor.close()
        connection.close()
        return False


def insert_data_into_redshift(ti):
    """Insert data into Redshift."""
    try:
        redshift_hook = RedshiftSQLHook("redshift_conn_id")
        connection = redshift_hook.get_conn()
        cursor = connection.cursor()

        # Insert genre KPIs
        genre_kpis = ti.xcom_pull(key="genre_kpis")
        for _, row in genre_kpis.iterrows():
            cursor.execute(
                """
                INSERT INTO genre_kpis 
                (date, genre, listen_count, avg_track_duration, avg_popularity, popularity_index, most_popular_track, timestamp) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['date'], row['track_genre'], row['listen_count'], row['avg_track_duration'], 
                 row['avg_popularity'], row['popularity_index'], row['most_popular_track'], now)
            )

        # Insert hourly KPIs
        hourly_kpis = ti.xcom_pull(key="hourly_kpis")
        for _, row in hourly_kpis.iterrows():
            cursor.execute(
                """
                INSERT INTO hourly_kpis 
                (listen_date ,hour, unique_listeners, total_plays, unique_tracks, track_diversity_index, top_artist, timestamp) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (row['date'], row['hour'], row['unique_listeners'], row['total_plays'], 
                 row['unique_tracks'], row['track_diversity_index'], row['top_artist'], now)
            )

        connection.commit()
        cursor.close()
        connection.close()
        print("✅ Data inserted into Redshift successfully.")
        return True
    except Exception as e:
        print(f"Failed to insert data into Redshift: {e}")
        cursor.close()
        connection.close()
        return False
    

