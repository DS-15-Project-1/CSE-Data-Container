import os
from obspy import read
import dask.dataframe as dd
import pandas as pd

def read_miniseed(file_path):
    stream = read(file_path)
    # Convert to DataFrame
    df = pd.DataFrame(stream[0].data)
    # Convert to Dask DataFrame
    ddf = dd.from_pandas(df, npartitions=4)
    return ddf

def main(directory):
    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".mseed"):
            file_path = os.path.join(directory, filename)
            ddf = read_miniseed(file_path)
            
            # Save the Dask DataFrame to a CSV file
            output_file = os.path.join(directory, f"processed_{filename}.csv")
            ddf.compute().to_csv(output_file, index=False)

if __name__ == "__main__":
    # Replace '/app/data' with the directory you wish to process
    main("/app/data/Run_7/Run_7/2021_01_21")
