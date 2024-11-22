import os
import obspy
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_miniseed_to_parquet(input_file, output_file):
   print("Script started")
   print(f"Converting: {input_file}")
   try:
        # Read the miniseed file
        st = obspy.read(input_file)

        # Convert to DataFrame
        data = []
        for tr in st:
            data.append({
                'network': tr.stats.network,
                'station': tr.stats.station,
                'location': tr.stats.location,
                'channel': tr.stats.channel,
                'starttime': tr.stats.starttime,
                'endtime': tr.stats.endtime,
                'sampling_rate': tr.stats.sampling_rate,
                'data': tr.data.tolist()
            })

        df = pd.DataFrame(data)

        # Write to Parquet
        pq.write_file(pa.Table.from_pandas(df), output_file)
        print(f"Successfully converted: {input_file} -> {output_file}")
    except Exception as e:
        print(f"Error converting {input_file}: {str(e)}")

# Set the input and output directories
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
output_dir = "/mnt/code/output"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Iterate over the directory structure
for root, dirs, files in os.walk(input_dir):
    print(f"Searching for mseed files in: {root}")
    for file in files:
        if file.endswith(".mseed"):
            input_file = os.path.join(root, file)
            rel_path = os.path.relpath(input_file, input_dir)
            output_file = os.path.join(output_dir, rel_path).replace(".mseed", ".parquet")
            
            # Create the output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Convert the file
            convert_miniseed_to_parquet(input_file, output_file)

print("Conversion complete!")
