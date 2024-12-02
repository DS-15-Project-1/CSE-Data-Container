import os
from pathlib import Path
import obspy
import pyarrow
import pandas as pd

def inspect_directory(input_dir):
    print(f"Inspecting directory: {input_dir}")
    try:
        print("Contents of the directory:")
        for item in os.listdir(input_dir):
            full_path = os.path.join(input_dir, item)
            print(f"  - {item} (is file: {os.path.isfile(full_path)})")
        
        if not os.listdir(input_dir):
            print(f"\nDirectory {input_dir} is empty.")
        
    except FileNotFoundError:
        print(f"Error: Directory {input_dir} not found.")
    except PermissionError:
        print(f"Error: Permission denied to access directory {input_dir}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def process_directory(input_dir, output_dir):
    print(f"Processing directory: {input_dir}")
    try:
        for root, dirs, files in os.walk(input_dir):
            for file in files:
                input_file = os.path.join(root, file)
                try:
                    print(f"Processing file: {input_file}")
                    
                    # Read the mseed file
                    st = obspy.read(input_file)
                    
                    # Convert to pandas DataFrame
                    df = pd.DataFrame({
                        'time': st[0].times('matplotlib'),
                        'data': st[0].data
                    })
                    
                    # Create output file path
                    rel_path = os.path.relpath(input_file, input_dir)
                    output_file = os.path.join(output_dir, rel_path).replace(os.path.splitext(file)[1], ".parquet")
                    
                    # Write to Parquet
                    df.to_parquet(output_file)
                    
                    print(f"File processed: {input_file}")
                    print(f"Output file: {output_file}")
                
                except Exception as e:
                    print(f"Error processing {input_file}: {str(e)}")
        
        print("Directory processing complete.")
    
    except Exception as e:
        print(f"Error processing directory: {str(e)}")

def main():
    input_dir = "/mnt/data/SWP_Seismic_Database_Current/2019/ZZ"
    output_dir = "/mnt/code/output"
    
    print(f"INPUT_DIR: {input_dir}")
    print(f"OUTPUT_DIR: {output_dir}")
    
    inspect_directory(input_dir)
    process_directory(input_dir, output_dir)

    print("\nConversion complete!")
    print(f"Total files processed: {len(os.listdir(input_dir))}")
    print(f"Files successfully converted: {len(os.listdir(output_dir))}")
    print(f"Output directory: {output_dir}")

if __name__ == "__main__":
    main()
