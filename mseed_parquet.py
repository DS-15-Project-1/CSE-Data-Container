import os
from obspy import read
import pandas as pd
import traceback

def convert_file_to_parquet(input_file_path, output_file_path):
    """
    Converts a miniSEED file to a Parquet file.

    Args:
        input_file_path (str): Path to the input miniSEED file.
        output_file_path (str): Path to the output Parquet file.
    """
    try:
        if not os.path.exists(input_file_path):
            raise FileNotFoundError(f"Input file '{input_file_path}' does not exist")
        if not os.path.isfile(input_file_path):
            raise ValueError(f"Input file '{input_file_path}' is not a regular file")

        # Read the miniSEED file
        stream = read(input_file_path)

        # Extract the metadata
        network = stream[0].stats.network
        station = stream[0].stats.station
        location = stream[0].stats.location
        channel = stream[0].stats.channel
        start_time = stream[0].stats.starttime
        end_time = stream[0].stats.endtime
        sampling_rate = stream[0].stats.sampling_rate

        # Create a Pandas DataFrame with the data
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [start_time],
            'endtime': [end_time],
            'sampling_rate': [sampling_rate],
            'data': [stream[0].data]
        })

        # Convert the start and end times to strings
        df['starttime'] = df['starttime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
        df['endtime'] = df['endtime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))

        # Write the DataFrame to a Parquet file
        if os.path.exists(output_file_path):
            existing_df = pd.read_parquet(output_file_path)
            combined_df = pd.concat([existing_df, df])
            combined_df.to_parquet(output_file_path, index=False)
        else:
            df.to_parquet(output_file_path, index=False)

        print(f"Successfully converted {input_file_path} to {output_file_path}")

    except Exception as e:
        print(f"Error converting {input_file_path}: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
    output_dir = "/mnt/code/output"

    os.makedirs(output_dir, exist_ok=True)

    for root, dirs, files in os.walk(input_dir):
        print(f"Searching for files in: {root}")
        for file in files:
            input_file_path = os.path.join(root, file)
            rel_path = os.path.relpath(input_file_path, input_dir)
            output_file_path = os.path.join(output_dir, rel_path).replace(os.path.splitext(file)[1], ".parquet")
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            convert_file_to_parquet(input_file_path, output_file_path)

    print("Conversion complete!")
