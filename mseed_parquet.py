import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback

def convert_file_to_parquet(input_file, output_file):
    print(f"Attempting to convert: {input_file}")
    try:
        # Debug logging
        print(f"Input file: {input_file}")
        print(f"Output file: {output_file}")
        print(f"File exists: {os.path.exists(input_file)}")
        print(f"Is file: {os.path.isfile(input_file)}")

        # Check file contents
        with open(input_file, 'rb') as f:
            print(f"File size: {os.path.getsize(input_file)} bytes")
            print(f"First few bytes: {f.read(20)}")

        # Read the file
        st = read(input_file)
        print(f"Successfully read: {input_file}")
        print(f"Number of traces: {len(st)}")
        print(f"Number of samples: {len(st[0].data)}")

        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        start_time = st[0].stats.starttime.isoformat()
        end_time = st[0].stats.endtime.isoformat()
        sampling_rate = st[0].stats.sampling_rate

        # Create DataFrame
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

        # Write to Parquet
        table = pa.Table.from_pandas(df)

        # Check if output file exists
        if os.path.exists(output_file):
            print(f"Output file already exists: {output_file}")
            existing_table = pq.read_table(output_file)
            combined_table = pa.concat_tables([existing_table, table])
            pq.write_table(combined_table, output_file)

        else:
            # if the file exists append the data to the existing file
            pq.write_table(table, output_file)

        # Debug logging
        print(f"Successfully converted: {input_file} -> {output_file}")

    except Exception as e:
        print(f"Error converting: {input_file}")
        print(traceback.format_exc())
        print(f"Error message: {str(e)}")
        print(f"Output file: {output_file}")
        print(f"Output file exists: {os.path.exists(output_file)}")
        print(f"Output file is file: {os.path.isfile(output_file)}")
        print(f"Output file size: {os.path.getsize(output_file)} bytes")
        print(f"First few bytes: {open(output_file, 'rb').read(20)}")

# Set the input and output directories
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
output_dir = "/mnt/code/output"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Iterate over the directory structure
for root, dirs, files in os.walk(input_dir):
    print(f"Searching for files in: {root}")
    for file in files:
        input_file = os.path.join(root, file)
        rel_path = os.path.relpath(input_file, input_dir)
        output_file = os.path.join(output_dir, rel_path).replace(os.path.splitext(file)[1], ".parquet")

        # Create the output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Convert the file
        convert_file_to_parquet(input_file, output_file)

print("Conversion complete!")