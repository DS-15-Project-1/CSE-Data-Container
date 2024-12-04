
import paramiko
import io
from obspy import read
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import traceback
import gc
from tqdm import tqdm
import time
import logging
import sys
import signal
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_sftp(hostname, username, password):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=hostname, username=username, password=password)
    sftp_client = ssh_client.open_sftp()
    return ssh_client, sftp_client

def disconnect_sftp(ssh_client, sftp_client):
    sftp_client.close()
    ssh_client.close()

def read_remote_file(sftp_client, remote_path):
    with sftp_client.open(remote_path, 'rb') as file:
        content = file.read()
    return io.BytesIO(content)

def write_remote_file(sftp_client, local_content, remote_path):
    with sftp_client.open(remote_path, 'wb') as file:
        file.write(local_content)

def remove_partial_files(sftp_client, directory_path, output_dir):
    partial_files = sftp_client.listdir(os.path.join(output_dir, directory_path))
    partial_files = [f for f in partial_files if f.startswith(f"{directory_path}_partial_") and f.endswith(".parquet")]

    for file in partial_files:
        file_path = os.path.join(output_dir, directory_path, file)
        sftp_client.remove(file_path)
        logger.info(f"Removed partial file: {file_path}")

def convert_file_to_parquet(sftp_client, input_file):
    logger.info(f"Attempting to convert: {input_file}")

    try:
        # Read the file remotely
        file_content = read_remote_file(sftp_client, input_file)

        # Read the file
        logger.info(f"Starting to read file: {input_file}")
        st = read(file_content, headonly=False)
        logger.info(f"Successfully read: {input_file}")

        # Extract metadata
        network = st[0].stats.network
        station = st[0].stats.station
        location = st[0].stats.location
        channel = st[0].stats.channel
        sampling_rate = st[0].stats.sampling_rate

        # Create DataFrame
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

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)

        logger.info(f"Successfully processed: {input_file}")
        return table

    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.warning(f"Skipping {input_file} and continuing with next file...")
        return None

def process_directory(sftp_client, directory_path, input_dir, output_dir):
    batch_start_time = time.time()

    dir_files = []
    for root, _, files in sftp_client.listdir_attr(os.path.join(input_dir, directory_path)):
        for file in files:
            rel_path = os.path.relpath(root, input_dir)
            dir_files.append((rel_path, file))

    logger.info(f"Processing directory: {directory_path} with {len(dir_files)} files")

    successful_conversions = 0
    failed_conversions = 0

    tables = []
    for rel_path, file in tqdm(dir_files, desc=f"Processing directory {directory_path}"):
        input_file = os.path.join(input_dir, rel_path, file)

        table = convert_file_to_parquet(sftp_client, input_file)

        if table is not None:
            tables.append(table)
            successful_conversions += 1

            # Write partial results every 10 successful conversions
            if successful_conversions % 10 == 0:
                partial_output_file = os.path.join(output_dir, directory_path, f"{directory_path}_partial_{successful_conversions}.parquet")
                partial_combined_table = pa.concat_tables(tables)

                # Convert table to bytes
                buffer = io.BytesIO()
                pq.write_table(partial_combined_table, buffer)
                buffer.seek(0)

                # Write to remote server
                write_remote_file(sftp_client, buffer.getvalue(), partial_output_file)
                logger.info(f"Wrote partial results to: {partial_output_file}")

        else:
            failed_conversions += 1

        if failed_conversions > len(dir_files) // 2:
            logger.critical(f"More than half of the files in {directory_path} failed conversion. Stopping.")
            return

    if tables:
        combined_table = pa.concat_tables(tables)
        output_file = os.path.join(output_dir, directory_path, f"{directory_path}.parquet")
        buffer = io.BytesIO()
        pq.write_table(combined_table, buffer)
        buffer.seek(0)
        write_remote_file(sftp_client, buffer.getvalue(), output_file)
        logger.info(f"Successfully wrote combined data to: {output_file}")

        # Remove partial files
        remove_partial_files(sftp_client, directory_path, output_dir)
        logger.info(f"Removed partial files for directory: {directory_path}")

    batch_end_time = time.time()
    batch_duration = batch_end_time - batch_start_time
    logger.info(f"Directory {directory_path} processed in {batch_duration:.2f} seconds")
    logger.info(f"Successful conversions: {successful_conversions}, Failed conversions: {failed_conversions}")

if __name__ == "__main__":
    try:
        hostname = '129.138.10.44'
        username = 'rob'
        password = 'prrc5142'

        ssh_client, sftp_client = connect_sftp(hostname, username, password)

        input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019/ZZ/FWU1/"
        output_dir = "/mnt/code/output"

        logger.info(f"Input directory: {input_dir}")
        logger.info(f"Output directory: {output_dir}")

        # Check if input directory exists remotely
        try:
            sftp_client.listdir(input_dir)
        except IOError:
            logger.error(f"Input directory does not exist: {input_dir}")
            logger.info(f"Contents of /mnt: {sftp_client.listdir('/mnt')}")
            logger.info(f"Contents of /mnt/data: {sftp_client.listdir('/mnt/data')}")
            sys.exit(1)

        subdirectories = [name for name in sftp_client.listdir(input_dir) if sftp_client.stat(os.path.join(input_dir, name)).st_mode & 0o40000]
        logger.info(f"Subdirectories found: {subdirectories}")

        if not subdirectories:
            logger.warning("No subdirectories found in the input directory")
            logger.info("Attempting to process files directly in the input directory")
            process_directory("", input_dir, output_dir)
        else:
            for subdir in tqdm(subdirectories, desc="Processing subdirectories"):
                process_directory(subdir, input_dir, output_dir)

        logger.info(f"Contents of input directory: {sftp_client.listdir(input_dir)}")
        logger.info("Conversion complete!")
        logger.info(f"Contents of output directory: {sftp_client.listdir('/mnt/code/output')}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

    finally:
        disconnect_sftp(ssh_client, sftp_client)

if tables:
    combined_table = pa.concat_tables(tables)
    output_file = os.path.join(output_dir, directory_path, f"{directory_path}.parquet")
    buffer = io.BytesIO()
    pq.write_table(combined_table, buffer)
    buffer.seek(0)
    write_remote_file(sftp_client, buffer.getvalue(), output_file)
    logger.info(f"Successfully wrote combined data to: {output_file}")

    # Remove partial files
    remove_partial_files(sftp_client, directory_path, output_dir)
    logger.info(f"Removed partial files for directory: {directory_path}")
