import os
import shutil
from pathlib import Path
from datetime import datetime
import concurrent
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import Future, ThreadPoolExecutor
import subprocess


def get_scratch_dir():
    """
    Helper method to return the scratch dir
    Recommended local mount point of NVMe or SSD

    """

    tmp_dir = "/scratch"
    return tmp_dir


def get_nworkers():
    '''
    Capture total cores on compute host
    MAY BE MOOT IF SUBMITTED THROUGH SLURM JOB QUEUE

    Total - 1 for admin
    '''
    cpu_cores = os.cpu_count()
    cpu_cores = max(1, cpu_cores - 1)
    return cpu_cores


def run_commands_concurrently(function, compute_keys, workers):
    """This method uses the ProcessPoolExecutor library to run
    multiple processes at the same time. It also has a debug option.
    This is helpful to show errors on stdout. 

    :param function: the function to run
    :param file_keys: tuple of file information
    :param workers: integer number of workers to use
    """
    *_, debug = compute_keys
    if debug:
        for compute_key in sorted(compute_keys):
            function(compute_key)
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            executor.map(function, sorted(compute_keys))
            executor.shutdown(wait=True)


def delete_in_background(path: str) -> Future:
    current_date = datetime.now().strftime('%Y-%m-%d')
    old_path = f"{path}.old_{current_date}"

    if os.path.exists(old_path): #JUST IN CASE >1 PROCESSED IN SINGLE DAY
        shutil.rmtree(old_path)

    os.rename(path, old_path)  # Rename the directory

    # Delete the renamed directory in the background
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(shutil.rmtree, old_path)
    return future


def move_files_in_background(glob_mask: str, src_path: Path, dest_path: Path, mode : str = 'move', debug : bool = False) -> None:
    '''
    Moves or copies (depending on mode) all files [matching glob_mask] from src_path to dest_path in the background
    '''
    
    # Find all files matching the glob mask in the source path
    files_to_move = list(src_path.glob('*' + glob_mask))
    if not files_to_move:
        print(f"No files found in {src_path} matching {glob_mask}")
        return

    if debug:
        print(f"Found {len(files_to_move)} files to move from {src_path} to {dest_path}.")

    # Function to move a single file
    def move_file(file: Path) -> None:
        # Check if file exists before moving
        if not file.exists():
            if debug:
                print(f"File does not exist: {file}")
            return
        
        if shutil.which("rclone") is None:
            if debug:
                print("Error: rclone is not installed or not in the system's PATH.")
            return
        try:
            if mode == 'move':
                subprocess.run(
                    ["rclone", "move", str(file), str(dest_path)],
                    check=True,
                )
            else:
                subprocess.run(
                    ["rclone", "copy", str(file), str(dest_path)],
                    check=True,
                )
            print(f"Moved: {file} -> {dest_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error moving {file}: {e}")

    # Use ThreadPoolExecutor to move files in parallel
    with ThreadPoolExecutor() as executor:
        executor.map(move_file, files_to_move)