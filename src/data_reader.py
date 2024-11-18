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
