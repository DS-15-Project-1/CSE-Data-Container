def process_directory(directory_path, input_dir, output_dir):
    batch_start_time = time.time()
    
    dir_files = []
    for root, _, files in os.walk(os.path.join(input_dir, directory_path)):
        for file in files:
            rel_path = os.path.relpath(root, input_dir)
            dir_files.append(os.path.join(rel_path, file))
    
    logger.info(f"Processing directory: {directory_path} with {len(dir_files)} files")
    
    all_data = []
    successful_conversions = 0
    failed_conversions = 0
    
    for file in tqdm(dir_files, desc=f"Processing directory {directory_path}"):
        input_file = os.path.join(input_dir, file)
        success, data = convert_file_to_parquet(input_file)
        
        if success:
            all_data.append(data)
            successful_conversions += 1
        else:
            failed_conversions += 1
        
        if failed_conversions > len(dir_files) // 2:
            logger.critical(f"More than half of the files in {directory_path} failed conversion. Stopping.")
            return
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        output_file = os.path.join(output_dir, f"{directory_path}.parquet")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        logger.info(f"Converting combined DataFrame to PyArrow Table")
        table = pa.Table.from_pandas(combined_df)
        
        logger.info(f"Writing combined table to Parquet: {output_file}")
        pq.write_table(table, output_file)
        
        logger.info(f"Successfully processed directory {directory_path} -> {output_file}")
    
    batch_end_time = time.time()
    batch_duration = batch_end_time - batch_start_time
    logger.info(f"Directory {directory_path} processed in {batch_duration:.2f} seconds")
    logger.info(f"Successful conversions: {successful_conversions}, Failed conversions: {failed_conversions}")

def convert_file_to_parquet(input_file):
    logger.info(f"Attempting to convert: {input_file}")
    
    try:
        # Get file size
        file_size = os.path.getsize(input_file)
        logger.info(f"File size: {file_size} bytes")
        
        if file_size > 1024 * 1024 * 1024:  # 1 GB limit
            logger.warning(f"File too large ({file_size} bytes), skipping: {input_file}")
            return False, None
        
        # Read the file
        logger.info(f"Starting to read file: {input_file}")
        st = read(input_file, headonly=False)
        logger.info(f"Successfully read: {input_file}")
        
        # Log additional information about the stream
        logger.info(f"Stream info: {st}")
        
        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        sampling_rate = st[0].stats.sampling_rate
        
        logger.info(f"Creating DataFrame")
        df = pd.DataFrame({
            'network': [network],
            'station': [station],
            'location': [location],
            'channel': [channel],
            'starttime': [st[0].stats.starttime.isoformat()],
            'endtime': [st[0].stats.endtime.isoformat()],
            'sampling_rate': [sampling_rate],
            'data': [st[0].data]
        })
        
        return True, df
    
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.warning(f"Skipping {input_file} and continuing with next file...")
        return False, None