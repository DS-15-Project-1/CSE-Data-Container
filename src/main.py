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

def main():
    # Read miniSEED files
    file_path = "/app/data/your_file.mseed"
    ddf = read_miniseed(file_path)
    
    # Save the Dask DataFrame to a file that can be served by Nginx
    ddf.compute().to_csv("/app/data/processed_data.csv", index=False)

if __name__ == "__main__":
    main()
