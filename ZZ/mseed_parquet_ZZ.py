import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback
from datetime import datetime, timedelta

def read_file_in_chunks(input_file, chunk_size=1000000):
    with open(input_file, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk

def process_miniseed_file(input_file):
    try:
        # Debug logging
        print(f"Input file: {input_file}")
        print(f"File exists: {os.path.exists(input_file)}")
        print(f"Is file: {os.path.isfile(input_file)}")
        
        # Check file readability
        if os.access(input_file, os.R_OK):
            print(f"File is readable: {input_file}")
        else:
            print(f"File is not readable: {input_file}")
            return None
        
        # Check file contents
        with open(input_file, 'rb') as f:
            print(f"File size: {os.path.getsize(input_file)} bytes")
            print(f"First few bytes: {f.read(20)}")
        
        # Read the file in chunks
        chunks = read_file_in_chunks(input_file)
        
        # Process the first chunk to get metadata
        first_chunk = next(chunks)
        try:
            st = read(first_chunk)
        except Exception as e:
            print(f"Error reading file {input_file}: {str(e)}")
            return None
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        start_time = st[0].stats.starttime
        end_time = st[0].stats.endtime
        sampling_rate = st[0].stats.sampling_rate
        
        # Convert UTCDateTime to datetime
        start_time = datetime.fromtimestamp(start_time.timestamp)
        end_time = datetime.fromtimestamp(end_time.timestamp)
        
        # Generate time series
        time_step = timedelta(seconds=1 / sampling_rate)
        time_series = pd.date_range(start=start_time, periods=len(st[0].data), freq=time_step)
        
        # Convert time_series to a list of timestamps
        timestamps = time_series.to_list()

        # Create DataFrame
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [start_time],
            'endtime': [end_time],
            'sampling_rate': [sampling_rate],
            'data': [st[0].data],
            'timestamps': [timestamps]
        })
        
        # Convert DataFrame to PyArrow Table
        schema = pa.schema([
            ('network', pa.string()),
            ('station', pa.string()),
            ('location', pa.string()),
            ('channel', pa.string()),
            ('starttime', pa.timestamp('ns')),
            ('endtime', pa.timestamp('ns')),
            ('sampling_rate', pa.float64()),
            ('data', pa.list_(pa.float64())),
            ('timestamps', pa.list_(pa.timestamp('ns')))
        ])
        
        table = pa.Table.from_pandas(df, schema=schema)
        
        # Write to Parquet
        output_file = os.path.splitext(input_file)[0] + '.parquet'
        pq.write_table(table, output_file)
        print(f"Successfully converted: {input_file} -> {output_file}")
    except Exception as e:
        print(f"Error processing file {input_file}: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")
        return None

def process_directory(input_dir, output_dir):
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over the directory structure
    for root, dirs, files in os.walk(input_dir):
        print(f"Searching for files in: {root}")
        for file in files:
            input_file = os.path.join(root, file)
            rel_path = os.path.relpath(input_file, input_file)
            output_file = os.path.join(output_dir, rel_path).replace(os.path.splitext(file)[1], ".parquet")
            
            # Create the output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Convert the file
            result = process_miniseed_file(input_file)
            if result is not None:
                print(f"Processed {input_file}")
            else:
                print(f"Failed to process {input_file}")

# Set the input and output directories
input_dir = r"/mnt/data/SWP_Seismic_Database_Current/2019/ZZ"
output_dir = r"/mnt/code/output/ZZ"

# Process the directory
process_directory(input_dir, output_dir)

print("Conversion complete!")
