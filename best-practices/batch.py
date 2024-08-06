import os
import sys
import boto3
import pickle
import pandas as pd

def setup_s3_fs():
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE', 'localstack'))
    s3 = session.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'))
    
    credentials = session.get_credentials()

    # Credentials are obtained explicitly
    aws_access_key_id = credentials.access_key
    aws_secret_access_key = credentials.secret_key
    aws_session_token = credentials.token

    # s3fs storage options
    storage_options = {
        'key': aws_access_key_id,
        'secret': aws_secret_access_key,
        'client_kwargs': {
            'endpoint_url': s3.meta.endpoint_url
        },
        'token': aws_session_token if aws_session_token is not None else None,
    }
    
    return storage_options

def read_data(year, month):
    storage_options = setup_s3_fs()
    input_file_pattern = os.getenv('INPUT_FILE_PATTERN')
    input_path = input_file_pattern.format(year=year, month=month) # type: ignore
    return pd.read_parquet(input_path, storage_options=storage_options)

def write_data(df, year, month):
    storage_options = setup_s3_fs()
    output_file_pattern = os.getenv('OUTPUT_FILE_PATTERN')
    output_path = output_file_pattern.format(year=year, month=month) # type: ignore
    df.to_parquet(output_path, engine='pyarrow', index=False, storage_options=storage_options)
    
    df.to_parquet(output_path, storage_options=storage_options, index=False)


def prepare_data(df, categorical):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


def main(year, month):
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    categorical = ['PULocationID', 'DOLocationID']
    df = read_data(year, month)
    df_prepared = prepare_data(df, categorical)
    df_prepared['ride_id'] = f'{year:04d}/{month:02d}_' + df_prepared.index.astype('str')

    dicts = df_prepared[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted sum duration:', y_pred.sum())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df_prepared['ride_id']
    df_result['predicted_duration'] = y_pred

    write_data(df_result, year, month)

if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)




# import os
# import sys
# import boto3
# import pickle
# import pandas as pd

# def get_input_path(year, month):
#     default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
#     input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
#     return input_pattern.format(year=year, month=month)


# def get_output_path(year, month):
#     default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
#     output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
#     return output_pattern.format(year=year, month=month)


# aws_profile = os.getenv('AWS_PROFILE', 'default')  # Profile set to 'localstack' but can be dynamically changed.

# def setup_s3_fs():
#     session = boto3.Session(profile_name=os.getenv('AWS_PROFILE', 'localstack'))
#     s3 = session.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566'))
    
#     credentials = session.get_credentials()

#     # Credentials are obtained explicitly
#     aws_access_key_id = credentials.access_key
#     aws_secret_access_key = credentials.secret_key
#     aws_session_token = credentials.token

#     # s3fs storage options
#     storage_options = {
#         'key': aws_access_key_id,
#         'secret': aws_secret_access_key,
#         'client_kwargs': {
#             'endpoint_url': s3.meta.endpoint_url
#         },
#         'token': aws_session_token if aws_session_token is not None else None,
#     }
    
#     return storage_options

# def read_data(year, month):
#     storage_options = setup_s3_fs()
#     input_file_pattern = os.getenv('INPUT_FILE_PATTERN')
#     input_path = input_file_pattern.format(year=year, month=month)
    
#     df = pd.read_parquet(input_path, storage_options=storage_options)
#     return df

# def save_data(df, year, month):
#     storage_options = setup_s3_fs()
#     output_file_pattern = os.getenv('OUTPUT_FILE_PATTERN')
#     output_path = output_file_pattern.format(year=year, month=month)
    
#     df.to_parquet(output_path, storage_options=storage_options, index=False)


# # def read_data(filename):
# #     # Read parquet file using pandas with specified profile options
# #     return pd.read_parquet(filename, storage_options=get_storage_options())

# # def save_data(df, filename):
# #     # Save to Parquet file
# #     df.to_parquet(filename, engine='pyarrow', index=False, storage_options=get_storage_options())

# # def read_data(filename):
# #     aws_profile = os.getenv('AWS_PROFILE', 'default')  # Profile set to 'localstack' but can be dynamically changed.
# #     s3_endpoint_url = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')  # Ensure correct LocalStack URL

# #     # Create a session using a specific profile
# #     session = boto3.Session(profile_name=aws_profile)
# #     credentials = session.get_credentials()

# #     # Obtain explicit credentials
# #     aws_access_key_id = credentials.access_key
# #     aws_secret_access_key = credentials.secret_key
# #     aws_session_token = credentials.token  # This might be None if not using temporary credentials

# #     # Define storage options for s3fs (used by pandas)
# #     storage_options = {
# #         'key': aws_access_key_id,
# #         'secret': aws_secret_access_key,
# #         'client_kwargs': {
# #             'endpoint_url': s3_endpoint_url,
# #         }
# #     }
    
# #     # Only include token if it is not None
# #     if aws_session_token:
# #         storage_options['token'] = aws_session_token

# #     # Read parquet file using pandas with specified profile options
# #     return pd.read_parquet(filename, storage_options=storage_options)

# # def save_data(df, filename):
# #     aws_profile = os.getenv('AWS_PROFILE', 'default')
# #     s3_endpoint_url = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')

# #     # Repeating steps from read_data to maintain consistency in credentials use
# #     session = boto3.Session(profile_name=aws_profile)
# #     credentials = session.get_credentials()

# #     aws_access_key_id = credentials.access_key
# #     aws_secret_access_key = credentials.secret_key
# #     aws_session_token = credentials.token

# #     storage_options = {
# #         'key': aws_access_key_id,
# #         'secret': aws_secret_access_key,
# #         'client_kwargs': {
# #             'endpoint_url': s3_endpoint_url,
# #         }
# #     }
    
# #     if aws_session_token:
# #         storage_options['token'] = aws_session_token

# #     # Save to Parquet file
# #     df.to_parquet(filename, engine='pyarrow', index=False, storage_options=storage_options)


# def prepare_data(df, categorical):
#     df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
#     df['duration'] = df.duration.dt.total_seconds() / 60
#     df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
#     df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
#     return df

# def main(year, month):
#     input_file = get_input_path(year, month)
#     output_file = get_output_path(year, month)

#     with open('model.bin', 'rb') as f_in:
#         dv, lr = pickle.load(f_in)

#     categorical = ['PULocationID', 'DOLocationID']
#     df = read_data(input_file, year, month)
#     df_prepared = prepare_data(df, categorical)
#     df_prepared['ride_id'] = f'{year:04d}/{month:02d}_' + df_prepared.index.astype('str')

#     dicts = df_prepared[categorical].to_dict(orient='records')
#     X_val = dv.transform(dicts)
#     y_pred = lr.predict(X_val)

#     print('predicted mean duration:', y_pred.mean())

#     df_result = pd.DataFrame()
#     df_result['ride_id'] = df_prepared['ride_id']
#     df_result['predicted_duration'] = y_pred

#     df_result.to_parquet(output_file, engine='pyarrow', index=False)

# if __name__ == '__main__':
#     year = int(sys.argv[1])
#     month = int(sys.argv[2])
#     main(year, month)





# import os
# import sys
# import pickle
# import pandas as pd

# import boto3

# def get_s3_client():
#     s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
#     print(s3_endpoint_url)
#     boto3.setup_default_session(profile_name='localstack')  
    
#     session = boto3.Session(
#         aws_access_key_id='test',
#         aws_secret_access_key='test',
#         region_name='us-east-1'
#     )
#     if s3_endpoint_url:
#         s3 = session.resource('s3', endpoint_url=s3_endpoint_url)
#     else:
#         s3 = session.resource('s3')
#     return s3

# s3 = get_s3_client()

# def list_buckets(s3):
#     for bucket in s3.buckets.all():
#         print(bucket.name)

# list_buckets(s3)


# def get_input_path(year, month):
#     default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
#     input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
#     return input_pattern.format(year=year, month=month)

# def get_output_path(year, month):
#     default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
#     output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
#     return output_pattern.format(year=year, month=month)

# def read_data(filename):
#     # Retrieve the endpoint URL from an environment variable (if specified)
#     s3_endpoint_url = os.getenv('S3_ENDPOINT_URL', None)
    
#     # Setup storage options if endpoint URL is provided
#     if s3_endpoint_url:
#         options = {'client_kwargs': {'endpoint_url': s3_endpoint_url}}
#     else:
#         options = None
    
#     # Read the Parquet file from S3 with potential LocalStack configuration
#     df = pd.read_parquet(filename, storage_options=options)
#     return df

# def prepare_data(df, categorical):
#     df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
#     df['duration'] = df.duration.dt.total_seconds() / 60
#     df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
#     df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
#     return df

# def main(year, month):
#     input_file = get_input_path(year, month)
#     output_file = get_output_path(year, month)

#     with open('model.bin', 'rb') as f_in:
#         dv, lr = pickle.load(f_in)

#     categorical = ['PULocationID', 'DOLocationID']
#     df = read_data(input_file)
#     df_prepared = prepare_data(df, categorical)
#     df_prepared['ride_id'] = f'{year:04d}/{month:02d}_' + df_prepared.index.astype('str')

#     dicts = df_prepared[categorical].to_dict(orient='records')
#     X_val = dv.transform(dicts)
#     y_pred = lr.predict(X_val)

#     print('predicted mean duration:', y_pred.mean())

#     df_result = pd.DataFrame()
#     df_result['ride_id'] = df_prepared['ride_id']
#     df_result['predicted_duration'] = y_pred

#     df_result.to_parquet(output_file, engine='pyarrow', index=False)

# if __name__ == '__main__':
#     year = int(sys.argv[1])
#     month = int(sys.argv[2])
#     main(year, month)


