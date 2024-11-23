import os
from obspy import read
from obspy.core.stream import Stream

def get_record_info(file_path):
    try:
        st = read(file_path)
        return st.get_record_information()
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return None

def process_directory(directory):
    all_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)
    
    for file_path in all_files:
        info = get_record_info(file_path)
        if info:
            print(f"File: {file_path}")
            print(info)
            print("\n")

# Specify the base directory
base_directory = '/mnt/data/SWP_Database_Current/2019/ZZ/'

# Process all files in the directory and its subdirectories
process_directory(base_directory)
