import os
import sys
from obspy import read
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as pf

def read_miniseed(file_path):
    stream = read(file_path)
    df = pd.DataFrame(stream[0].data)
    ddf = dd.from_pandas(df, npartitions=4)
    return ddf

def process_miniseed_file(file_path, output_directory):
    ddf = read_miniseed(file_path)
    df = ddf.compute()
    
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    
    output_csv_file = os.path.join(output_directory, f"{base_name}.csv")
    output_parquet_file = os.path.join(output_directory, f"{base_name}.parquet")
    output_feather_file = os.path.join(output_directory, f"{base_name}.feather")
    output_json_file = os.path.join(output_directory, f"{base_name}.json")
    
    # Save as CSV
    df.to_csv(output_csv_file, index=False)
    
    # Save as Parquet
    pq.write_table(pa.Table.from_pandas(df), output_parquet_file)
    
    # Save as Feather
    pf.write_file(pa.Table.from_pandas(df), output_feather_file)

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
