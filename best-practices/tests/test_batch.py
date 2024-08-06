from datetime import datetime
import pandas as pd
import sys

sys.path.append('/workspaces/mlops-zoomcamp/best-practices/')
from batch import prepare_data

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def prepare_data(df, categorical):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    df = df.reset_index(drop=True)  # Reset the index after filtering
    return df

def test_prepare_data():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)

    categorical = ['PULocationID', 'DOLocationID']
    processed_df = prepare_data(df, categorical)

    expected_data = [
        {'PULocationID': '-1', 'DOLocationID': '-1', 'duration': 9.0, 'ride_id': '2023/01_0'},
        {'PULocationID': '1', 'DOLocationID': '1', 'duration': 8.0, 'ride_id': '2023/01_1'}
    ]

    processed_df['ride_id'] = '2023/01_' + processed_df.index.astype('str')
    
    actual_data = processed_df[['PULocationID', 'DOLocationID', 'duration', 'ride_id']].to_dict(orient='records')
    
    assert actual_data == expected_data