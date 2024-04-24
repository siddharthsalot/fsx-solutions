#!/usr/bin/env python3

"""
Pre-requisite
    sudo yum install -y python3

The purpose of this script is to provide a list of directories that may be contributing to uneven load across metadata servers
during a workload execution. This script should be executed on an FSx Lustre client instance.

Depending on the number of files and directories on the target MDT, this script can take a long duration of time to complete.
It is recommended to:
* Run this script in a screen session OR
* Run the script in the background using nohup.

The script requires users to input to provide mdt-index denoting the MDT host under heavy load.

Usage:
analyze_mdt_io_hotspotting.py [-h]
        [--mount-point MOUNT_POINT]
        [--mdt-index MDT_INDEX]
        [--workload-start-time WORKFLOW_START_TIME]
        [--output-path OUTPUT_PATH]

For example,
python3 analyze_mdt_io_hotspotting.py \
    --mount-point /fsx
    --mdt-index 1 \
    --workload-start-time "2024-01-01 00:00:00"
    --output-path /tmp/output_dir

In above example, the MDT host under heavy load is MDT0001.
"""

import argparse
import csv
import concurrent.futures
import logging
import math
import multiprocessing
import os
import sys
import subprocess
import traceback
from datetime import datetime
from subprocess import PIPE

DEFAULT_MOUNT_POINT = "/fsx"
DEFAULT_CONCURRENCY = multiprocessing.cpu_count()
HEADER=["dir_name","dir_creation_date","num_files_accessed","total_num_files"]
SECONDS_TO_DAYS=60*60*24

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Helper Methods
def run_lfs_find(path, criteria):
    """
    Run lfs find command with specified criteria and return a list of file/directory paths.

    :param path: The path to search in.
    :param criteria: A list of criteria for the lfs find command.
    :return: A list of file/directory paths.
    """
    # Construct the command
    command = ['lfs', 'find', path] + criteria
    
    # Execute the command
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # Check if the command was executed successfully
        if result.returncode != 0:
            print(f"Error executing lfs find: {result.stderr}")
            return []
        
        # Split the output into lines and return the list
        file_paths = result.stdout.strip().split('\n')
        return file_paths
    except Exception as e:
        print(f"Failed to run lfs find command: {e}")
        return []

def count_files(path):
    """
    Count the total number of files directly under a directory (at depth 1).

    :param directory: The path to the directory to count files in.
    :return: The total number of files.
    """
    file_count = 0
    for _, _, files in os.walk(path):
        file_count += len(files)
        break  # Stop walking after the first iteration
    return file_count

def count_files_accessed_by_workload(path, args):
    time_diff_in_days = math.ceil(time_diff(args.workload_start_time)/60/60/24)
    """
    Count unique files in a directory at depth of 1 that have been accessed, modified,
    or had their metadata changed less than 1 day ago.

    :param directory: The path to the directory to search.
    :return: The count of unique files.
    """
    command = [
        'find', path,
        '-type', 'f',
        '-maxdepth', '1',
        '(',
        '-atime', f'-{time_diff_in_days}', '-o',  # Accessed less than X days ago
        '-mtime', f'-{time_diff_in_days}', '-o',  # Modified less than X days ago
        '-ctime', f'-{time_diff_in_days}',        # Metadata changed less than X days ago
        ')',
        '-print'
    ]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        files = result.stdout.strip().split('\n')
        return len(files)
    except subprocess.CalledProcessError as e:
        print(f"Error executing find command: {e.stderr}")
        return 0

def find_files_accessed_in_dir(dir_path, args):
    """
    Finds the number of files accessed by the workload and the total number of files at a directory level.
    :param dir_path: The path to the directory
    :param args: Script input arguments
    """
    if dir_path is None or dir_path == '':
        return None
    num_files_accessed = count_files_accessed_by_workload(dir_path, args)
    total_num_files = count_files(dir_path)
    dir_info = {
        "dir_name": dir_path,
        "dir_creation_date": datetime.fromtimestamp(os.path.getctime(dir_path)),
        "num_files_accessed": num_files_accessed,
        "total_num_files": total_num_files
    }

    return dir_info

def write_csv(row_dicts, filepath, header):
    """
    Writes a list of objects to a csv file in the provided file path with the provided header.
    :param row_dicts: A list of dictionary objects representing rows in a csv file.
    :param filepath: The output csv file path.
    :param header: A list containing the header objects for the csv file.
    """
    with open(filepath, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        writer.writerows(row_dicts)

def time_diff(workload_start_time):
    """
    Calculates the time difference in seconds between workload start time and the current time.
    :param workload_start_time - The start time of workload passed as script input in format "YYYY-MM-DD HH:MM:SS"
    :return: The time difference in seconds.
    """

    start_time = datetime.strptime(workload_start_time, '%Y-%m-%d %H:%M:%S')
    # Get the current date-time
    current_date_time = datetime.now()
    
    # Calculate the difference
    time_diff = current_date_time - start_time

    # Convert the difference in seconds
    return time_diff.total_seconds()

def find_dirs_on_mdt(args):
    """
    Finds directories/files on a given MDS host that were accessed since the workload start time.
    It generates a csv file in the given output path that consists of:
    <dir_name>,<creation_date>,<num_files_accessed>,<total_num_files>
    """
    search_path = args.mount_point
    time_diff_in_days = math.ceil(time_diff(args.workload_start_time)/SECONDS_TO_DAYS)

    # Find directories that were modified (mtime) since workload started executing.
    search_criteria_by_mtime = ["--type", "d", "--mdt", args.mdt_index, "--mtime", f"-{time_diff_in_days}"]

    # Get the list of directory paths
    directories_searched_by_mtime = run_lfs_find(search_path, search_criteria_by_mtime)

    # Create a union set of all directories that had most recent atime, mtime and ctime.
    directories_accessed = set(directories_searched_by_mtime)
    dir_info = []
    with concurrent.futures.ThreadPoolExecutor(max_workers = DEFAULT_CONCURRENCY) as executor:
        futures = []
        for directory in directories_accessed:
            futures.append(
                executor.submit(
                    find_files_accessed_in_dir, directory, args
                )
            )
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if res:
                    dir_info.append(future.result())
            except Exception as e:
                traceback.print_exc()
                logging.error(e)
    output_path = f'{args.output_path}/{datetime.now().date()}-files_dirs_accessed.csv'
    write_csv(dir_info, output_path, HEADER)
    logging.info(f"Saved output file to {output_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mount-point", help="FSx Lustre mount point on the client instance. For example: /mnt/lustre/client.")
    parser.add_argument("--mdt-index", help="The MDT index to analyze for load. For example: 1 indicates MDT0001")
    parser.add_argument("--workload-start-time", help="Workload start time in format YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--output-path", help="Output directory where the script will store the results in csv format.")
    
    args = parser.parse_args()

    # Logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        stream=sys.stdout,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f"Using mount point: {args.mount_point}")
    logging.info(f"Using MDT index: {args.mdt_index}")
    logging.info(f"Using Workload Start Time: {args.workload_start_time}")
    logging.info(f"Using Output Path: {args.output_path}")
    find_dirs_on_mdt(args)

if __name__ == "__main__":
    sys.exit(main())
