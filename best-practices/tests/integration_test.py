import os
import sys
import boto3
import pandas as pd
from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def create_dataframe():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2), dt(1, 2, 59)),
        (3, 4, dt(1, 2), dt(2, 2, 1)),
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    return pd.DataFrame(data, columns=columns)

# def prepare_data(df, categorical):
#     df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
#     df['duration'] = df.duration.dt.total_seconds() / 60
#     df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
#     df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
#     df = df.reset_index(drop=True)  # Reset the index after filtering
#     return df

# def test_prepare_data():
#     data = [
#         (None, None, dt(1, 1), dt(1, 10)),
#         (1, 1, dt(1, 2), dt(1, 10)),
#         (1, None, dt(1, 2, 0), dt(1, 2, 59)),
#         (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
#     ]
#     columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
#     df = pd.DataFrame(data, columns=columns)

#     return df

def setup_s3_bucket():
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE', 'localstack'))
    s3 = session.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'))
    bucket_name = os.getenv('S3_BUCKET_NAME', 'nyc-duration')
    
    try:
        # Check if bucket already exists
        existing_buckets = s3.list_buckets()
        if any(bucket['Name'] == bucket_name for bucket in existing_buckets.get('Buckets', [])):
            print("Bucket already exists.")
        else:
            s3.create_bucket(Bucket=bucket_name)
            print("Bucket created successfully.")
    except Exception as e:
        print(f"Error accessing or creating bucket: {e}")

    return f's3://{bucket_name}/'



def save_to_s3(df, s3_path):
    aws_credentials = {
        'key': os.getenv('AWS_ACCESS_KEY_ID'),
        'secret': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'token': os.getenv('AWS_SESSION_TOKEN'),    # This can be None if not using temporary credentials
        'client_kwargs': {
            'endpoint_url': os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
        }
    }

    df.to_parquet(
        s3_path,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=aws_credentials
    )
    print(f"DataFrame saved to {s3_path}")

def run_batch_script(year, month):
    # Assuming batch.py takes year and month as command line arguments
    command = f'python batch.py {year} {month}'
    os.system(command)

def main():
    bucket_path = setup_s3_bucket()
    df_input = create_dataframe()
    input_file = bucket_path + 'in/2023-01.parquet'
    save_to_s3(df_input, input_file)

    # Run the batch processing script
    run_batch_script(2023, 1)
    
    # Verification by read back
    df_output = pd.read_parquet(input_file, storage_options={
        'client_kwargs': {
            'endpoint_url': os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
        }})
    print("DataFrame read from S3:")
    print(df_output)

if __name__ == '__main__':
    main()
