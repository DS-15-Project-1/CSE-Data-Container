import os
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback
from datetime import datetime

def convert_channel_to_parquet(input_dir, output_dir):
    print(f"Processing channel data in {input_dir}")
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Print directory contents
    print(f"Contents of {input_dir}:")
    for item in os.listdir(input_dir):
        print(f"  {item}")

    # Iterate over all channels (HHZ, HHE, HHN)
    channels = ['HHZ', 'HHE', 'HHN']
    for channel in channels:
        # Create the output file name
        output_file = os.path.join(output_dir, f"{channel}.parquet")
        
        # Initialize lists to store data
        networks = []
        stations = []
        locations = []
        channels = []
        starttimes = []
        endtimes = []
        sampling_rates = []
        data = []
        timestamps = []
        
        # Process each file in the input directory
        channel_dir = os.path.join(input_dir, f"{channel}.D")
        if os.path.isdir(channel_dir):
            print(f"Processing channel directory: {channel_dir}")
            file_count = len(os.listdir(channel_dir))
            processed_count = 0
            
            for file in os.listdir(channel_dir):
                input_file = os.path.join(channel_dir, file)
                print(f"  Processing file {processed_count + 1}/{file_count}: {input_file}")
                
                try:
                    # Read the file
                    st = read(input_file)
                    
                    # Extract metadata
                    networks.append(st[0].stats.network)
                    stations.append(st[0].stats.station)
                    locations.append(st[0].stats.location)
                    channels.append(st[0].stats.channel)
                    starttimes.append(datetime.fromtimestamp(st[0].stats.starttime.timestamp))
                    endtimes.append(datetime.fromtimestamp(st[0].stats.endtime.timestamp))
                    sampling_rates.append(st[0].stats.sampling_rate)
                    data.append(st[0].data)
                    timestamps.append(st[0].times())
                    
                    processed_count += 1
                    
                    # Process in chunks of 100 files
                    if processed_count % 10 == 0:
                        process_chunk(networks, stations, locations, channels, starttimes, endtimes, sampling_rates, data, timestamps, output_file)
                        networks, stations, locations, channels, starttimes, endtimes, sampling_rates, data, timestamps = [], [], [], [], [], [], [], [], []
                        
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")
                    traceback.print_exc()
            
            # Process any remaining files
            if networks:
                process_chunk(networks, stations, locations, channels, starttimes, endtimes, sampling_rates, data, timestamps, output_file)
            
            print(f"Successfully converted {channel} data: {output_file}")
        else:
            print(f"Channel directory not found: {channel_dir}")

def process_chunk(networks, stations, locations, channels, starttimes, endtimes, sampling_rates, data, timestamps, output_file):
    # Create DataFrame
    df = pd.DataFrame({
        'network': networks,
        'station': stations,
        'location': locations,
        'channel': channels,
        'starttime': starttimes,
        'endtime': endtimes,
        'sampling_rate': sampling_rates,
        'data': data,
        'timestamps': timestamps
    })
    
    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # Write the table to Parquet file
    if os.path.exists(output_file):
        # Append to existing Parquet file
        existing_table = pq.read_table(output_file)
        combined_table = pa.concat_tables([existing_table, table])
        pq.write_table(combined_table, output_file)
    else:
        # Create new Parquet file
        pq.write_table(table, output_file)

# Usage
input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019/ZZ"
output_dir = "/mnt/code/output/ZZ"

# Process each station directory
for station in os.listdir(input_dir):
    station_input_dir = os.path.join(input_dir, station)
    station_output_dir = os.path.join(output_dir, station)
    print(f"Processing station: {station}")
    convert_channel_to_parquet(station_input_dir, station_output_dir)

print("Conversion complete!")
