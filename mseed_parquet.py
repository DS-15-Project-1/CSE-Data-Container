def convert_directory_to_parquet(directory, output_file):
    print(f"Processing directory: {directory}")
    dfs = []
    
    for root, _, files in os.walk(directory):
        for file in files:
            input_file = os.path.join(root, file)
            print(f"Attempting to convert: {input_file}")
            
            try:
                # Read the file
                st = read(input_file)
                print(f"Successfully read: {input_file}")
                
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
                    'data': [st[0].data],
                    'filename': [file]
                })
                
                dfs.append(df)
                print(f"DataFrame created for {input_file}")
            
            except Exception as e:
                print(f"Error converting {input_file}: {str(e)}")
                print(f"Traceback: {traceback.format_exc()}")
    
    if dfs:
        print(f"Number of DataFrames collected: {len(dfs)}")
        
        try:
            # Concatenate all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            print("DataFrames concatenated successfully")
            
            # Write to Parquet
            table = pa.Table.from_pandas(combined_df)
            pq.write_table(table, output_file)
            print(f"Successfully converted directory: {directory} -> {output_file}")
        except Exception as e:
            print(f"Error during DataFrame concatenation or Parquet writing: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
    else:
        print(f"No valid DataFrames collected from directory: {directory}")
        if __name__ == "__main__":
    input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019"
    output_dir = "/mnt/code/output"
    
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each subdirectory
    for subdir in os.listdir(input_dir):
        subdir_path = os.path.join(input_dir, subdir)
        if os.path.isdir(subdir_path):
            output_file = os.path.join(output_dir, f"{subdir}.parquet")
            print(f"Converting directory: {subdir_path}")
            convert_directory_to_parquet(subdir_path, output_file)
    
    print("Conversion complete!")