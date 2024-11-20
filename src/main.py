import os
import sys
from obspy import read
import dask.dataframe as dd
import pandas as pd

def read_miniseed(file_path):
    stream = read(file_path)
    df = pd.DataFrame(stream[0].data)
    ddf = dd.from_pandas(df, npartitions=4)
    return ddf

def main(input_directory, output_directory):
    for filename in os.listdir(input_directory):
        if filename.endswith(".mseed"):
            file_path = os.path.join(input_directory, filename)
            ddf = read_miniseed(file_path)
            
            output_file = os.path.join(output_directory, f"processed_{filename}.csv")
            ddf.compute().to_csv(output_file, index=False)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_directory> <output_directory>")
        sys.exit(1)
    
    input_directory = sys.argv[1]
    output_directory = sys.argv[2]
    main(input_directory, output_directory)
