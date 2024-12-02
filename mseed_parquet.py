import os
from obspy import read
import pandas as pd
import traceback

def convert_file_to_parquet(input_file, output_file):
    print(f"Attempting to convert: {input_file}")
    try:
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
        print(f"File exists: {os.path.exists(input_file)}")
        print(f"Is file: {os.path.isfile(input_file)}")
        with open(input_file, 'rb') as f:
            print(f"File size: {os.path.getsize(input_file)} bytes")
            print(f"First few bytes: {f.read(20)}")
        st = read(input_file)
        print(f"Successfully read: {input_file}")
        print(f"Number of traces: {len(st)}")
        print(f"Number of samples: {len(st[0].data)}")
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        start_time = st[0].stats.starttime
        end_time = st[0].stats.endtime
        sampling_rate = st[0].stats.sampling_rate
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [start_time],
            'endtime': [end_time],
            'sampling_rate': [sampling_rate],
            'data': [st[0].data]
        })
        df['starttime'] = df['starttime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
        df['endtime'] = df['endtime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
        if os.path.exists(output_file):
            existing_df = pd.read_parquet(output_file)
            combined_df = pd.concat([existing_df, df])
            combined_df.to_parquet(output_file, index=False)
        else:
            df.to_parquet(output_file, index=False)
        print(f"Successfully converted: {input_file} -> {output_file}")
    except Exception as e:
        print(f"Error converting {input_file}: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")

input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
output_dir = "/mnt/code/output"

os.makedirs(output_dir, exist_ok=True)

for root, dirs, files in os.walk(input_dir):
    print(f"Searching for files in: {root}")
    for file in files:
        input_file = os.path.join(root, file)
        rel_path = os.path.relpath(input_file, input_dir)
        output_file = os.path.join(output_dir, rel_path).replace(os.path.splitext(file)[1], ".parquet")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        convert_file_to_parquet(input_file, output_file)

print("Conversion complete!")