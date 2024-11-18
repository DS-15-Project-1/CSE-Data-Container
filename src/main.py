from data_reader import read_miniseed
from data_processor import process_data
import dask.dataframe as dd

def main():
    # Read miniSEED files
    ddf = read_miniseed("/notebooks/path/to/your/file.mseed")
    
    # Process data
    processed_ddf = process_data(ddf)
    
    # Save processed data
    processed_ddf.to_hdf5("/notebooks/data/processed_data.h5", key="data")

if __name__ == "__main__":
    main()
