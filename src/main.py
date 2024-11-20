import os
import sys
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

def read_miniseed(file_path):
    stream = read(file_path)
    return stream

def process_miniseed_file(file_path, output_directory):
    stream = read_miniseed(file_path)
    
    for trace in stream:
        data = trace.data
        sampling_rate = trace.stats.sampling_rate
        start_time = trace.stats.starttime
        end_time = trace.stats.endtime
        channel = trace.stats.channel
        station = trace.stats.station
        network = trace.stats.network
        location = trace.stats.location
        
        # Create time array
        time_array = pd.date_range(start_time.datetime, end_time.datetime, periods=len(data))
        
        # Create DataFrame
        df = pd.DataFrame({
            'time': time_array,
            'data': data,
            'sampling_rate': sampling_rate,
            'channel': channel,
            'station': station,
            'network': network,
            'location': location
        })
        
        # Apply scaling if necessary
        # Note: You may need to adjust this based on your specific data
        scaling_factor = trace.stats.calib
        if scaling_factor != 1.0:
            df['scaled_data'] = df['data'] * scaling_factor
        
        # Create output file name
        base_name = f"{network}_{station}_{location}_{channel}_{start_time.strftime('%Y%m%d_%H%M%S')}"
        output_parquet_file = os.path.join(output_directory, f"{base_name}.parquet")
        
        # Save as Parquet
        table = pa.Table.from_pandas(df)
        pq.write_file(table, output_parquet_file)
        
        print(f"Processed and saved: {output_parquet_file}")

def main(input_directory, output_directory):
    for filename in os.listdir(input_directory):
        if filename.endswith(".mseed"):
            file_path = os.path.join(input_directory, filename)
            process_miniseed_file(file_path, output_directory)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_directory> <output_directory>")
        sys.exit(1)

    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    main(input_directory, output_directory)
