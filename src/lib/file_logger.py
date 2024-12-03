"""
-Writes logging and debugging info to file on server.
-Also includes mgmt of meta-data and status json files
"""

import os
import logging
from datetime import datetime
from pathlib import Path
import json

class FileLogger:
    """This class defines the file logging mechanism
    the first instance of FileLogger class defines default log file name and complete path 'LOGFILE_PATH'
    The full path is passed during application execution (i.e., running the pre-processing pipeline) and sets an
    environment variable for future file logging

    Optional configuration (defined in __init__) provide for concurrent output to file and console [currently
    only file output]

    Single method [outside of __init__] in class accepts log message as argument, creates current timestamp and saves to file
    """

    def __init__(self, LOGFILE_PATH, debug=False):
        """
        -SET CONFIG FOR LOGGING TO FILE; ABILITY TO OUTPUT TO STD OUTPUT AND FILE

        """

        LOGFILE = os.path.join(LOGFILE_PATH)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Create a file handler
        file_handler = logging.FileHandler(LOGFILE)
        file_handler.setLevel(logging.DEBUG)

        # Create a formatter and add it to the handler
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # 'FOR LOOP' REMOVES DUAL LOGGING TO CONSOLE + FILE
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(file_handler)
        self.debug = debug

    def log_info(self, message):
        self.logger.info(message)

    def log_warning(self, message):
        self.logger.warning(message)

    def log_error(self, message):
        self.logger.error(message)


    def logevent(self, msg: str):
        '''
        Implements output to terminal if debug is set to True (similar to linux command: tee)
        
        :param msg: accepts string comment that gets inserted into file log
        :type msg: str
        :return: timestamp of event is returned [unclear if used as of 4-NO-2022]
        '''
        timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"{timestamp} - {msg}")
        if self.debug:
            print(f"{timestamp} - {msg}")
        return timestamp
    

    def read_metadata_status_files(self, base_input_location, debug):
        base_data_dir = [d for d in Path(base_input_location).iterdir() if d.is_dir()]  # List of Path objects for directories
        
        status_json_exists = {}
        subfolder_counts = {}
        
        for dir_name in base_data_dir:
            status_json_path = dir_name / "status.json"
        
            # Check if status.json exists
            if status_json_path.exists():
                status_json_exists[dir_name.name] = True

                if debug:
                    print(f'status.json exists for {dir_name}')

                # Load the existing status.json content
                with open(status_json_path, 'r') as status_file:
                    stored_subfolders = json.load(status_file)
                
                # Get the current subfolders in the directory
                current_subfolders = {
                    subfolder.name: subfolder
                    for subfolder in dir_name.iterdir() if subfolder.is_dir()
                }

                # Compare stored subfolders with current subfolders
                stored_subfolder_names = set(stored_subfolders.keys())

                # If subfolders have changed, update status.json
                if stored_subfolder_names != current_subfolders.keys():
                    print(f"Subfolders have changed in {dir_name.name}. Updating status.json.")
                    subfolders_dict = {
                        subfolder.name: {
                            "processed": (subfolder / "meta-data.json").exists(),  # Check if 'meta-data.json' exists
                            "folder_cnt": sum(1 for sub in subfolder.iterdir() if sub.is_dir())  # Count subdirectories
                        }
                        for subfolder in current_subfolders.values()
                    }
                    # Write the updated subfolder information to status.json
                    with open(status_json_path, 'w') as status_file:
                        json.dump(subfolders_dict, status_file, indent=4)
                    status_json_exists[dir_name.name] = False  # Mark it as updated
                else:
                    print("No subfolders count change.  Checking progress...")
                    
                    # Check for subfolders marked as 'processed': false and recheck for 'meta-data.json'
                    for subfolder_name, subfolder_info in stored_subfolders.items():
                        # Ensure the 'processed' key exists, defaulting to False if not
                        if subfolder_info.get("processed", False) is False:  # If processed is False, check for 'meta-data.json'
                            meta_data_filename = Path(dir_name, subfolder_name, "meta-data.json")
                            subfolder = Path(dir_name, subfolder_name)

                            if (meta_data_filename).exists():
                                print(f'Reading {meta_data_filename}')
                                task = self.read_individual_json_manifest(meta_data_filename, debug)

                                # Update the subfolder's folder count (number of subdirectories)
                                subfolder_info["folder_cnt"] = sum(1 for sub in subfolder.iterdir() if sub.is_dir())
                            else:
                                if debug:
                                    print(f'{meta_data_filename} does not exist. Creating')
                                task = self.create_individual_json_manifest(meta_data_filename, subfolder, debug)

                    # Save updated status.json after checking
                    with open(status_json_path, 'w') as status_file:
                        json.dump(stored_subfolders, status_file, indent=4)

                    # If there is at least one non-processed subfolder with more than 0 subfolders, create the result dictionary
                    for subfolder_name, subfolder_info in stored_subfolders.items():
                        if not subfolder_info["processed"] and subfolder_info["folder_cnt"] > 0:
                            if dir_name.name not in subfolder_counts:
                                subfolder_counts[dir_name.name] = {}
                            subfolder_counts[dir_name.name][subfolder_name] = [subfolder_info["folder_cnt"], task]
            
            else:
                print(f'Creating status.json in {dir_name.name}')
                status_json_exists[dir_name.name] = False
                
                # If status.json does not exist, create it
                subfolders_dict = {
                    subfolder.name: {
                        "processed": (subfolder / "meta-data.json").exists(),  # Check if 'meta-data.json' exists
                        "folder_cnt": sum(1 for sub in subfolder.iterdir() if sub.is_dir())  # Count subdirectories
                    }
                    for subfolder in dir_name.iterdir() if subfolder.is_dir()
                }
                
                # Write the subfolder information to status.json
                with open(status_json_path, 'w') as status_file:
                    json.dump(subfolders_dict, status_file, indent=4)
                
                # Check the newly created status.json for non-processed subfolders with > 0 subfolders
                for subfolder_name, subfolder_info in subfolders_dict.items():
                    if not subfolder_info["processed"] and subfolder_info["folder_cnt"] > 0:
                        if dir_name.name not in subfolder_counts:
                            subfolder_counts[dir_name.name] = {}
                        subfolder_counts[dir_name.name][subfolder_name] = [subfolder_info["folder_cnt"], task]

        return subfolder_counts
    

    def read_individual_json_manifest(self, meta_data_file_location, debug: bool):
        
        # Open the file and load its contents
        try:
            with open(meta_data_file_location, 'r') as status_file:
                folder_manifest = json.load(status_file)

            # Check if the 'avi_create' key exists in the loaded data
            if not 'last_task' in folder_manifest:
                print("'last_task' key not found. Adding 'last_task' with value 'create_json_manifest'.")
                # If 'last_task' key is not found, create it with the value 'create_json_manifest'
                folder_manifest['last_task'] = 'create_json_manifest'

                # Optionally, save the updated JSON back to the file
                with open(meta_data_file_location, 'w') as status_file:
                    json.dump(folder_manifest, status_file, indent=4)

                print(f"Updated 'last_task' to: {folder_manifest['last_task']}")

            return folder_manifest['last_task']
        except Exception as e:
            print(f"Error loading JSON file: {e}")


    def create_individual_json_manifest(self, meta_data_file_location, subfolder: Path, debug: bool):
        all_folders = [f.name for f in subfolder.iterdir() if f.is_dir()]
        all_folders.sort(key=lambda x: int(x) if x.isdigit() else x)
        meta = {}
        meta['folders'] = all_folders
        meta['last_task'] = 'create_json_manifest'
        try:
            with open(meta_data_file_location, 'w') as json_file:
                json.dump(meta, json_file, indent=4)
            
            if debug:
                print(f"Created JSON manifest at {meta_data_file_location}")
        except Exception as e:
            print(f"Error creating JSON manifest: {e}")
            
        return meta['last_task']
    

    def update_individual_json_manifest(self, meta_data_file_location, task: str):
        
        try:
            # Read the existing JSON file if it exists
            if Path(meta_data_file_location).exists():
                with open(meta_data_file_location, 'r') as json_file:
                    meta = json.load(json_file)

                # Update 'last_task' with the new task
                meta['last_task'] = task

                # Save the updated JSON back to the file
                with open(meta_data_file_location, 'w') as json_file:
                    json.dump(meta, json_file, indent=4)

                if self.debug:
                    print(f"Updated 'last_task' to '{task}' in {meta_data_file_location}")

                return meta['last_task']

        except Exception as e:
            print(f"Error updating JSON manifest: {e}")
            return ""
        
    
    def update_metadata_status_file(self, status_file_location: Path, entry: tuple):
        '''
        'entry' tuple has following structure: (session, 'processed', True)
        '''
        status_file_location = Path(status_file_location, 'status.json')
        
        if self.debug:
            print(f"Updating metadata status file: {status_file_location}")

        # Check if status.json exists
        if not status_file_location.exists():
            raise FileNotFoundError(f"status.json file does not exist at {status_file_location}")

        # Load the current status.json data
        with open(status_file_location, 'r') as status_file:
            status_data = json.load(status_file)

        # Extract the session, key, and value from the entry tuple
        session, key, value = entry

        # Validate that the session exists in the JSON data
        if session not in status_data:
            raise KeyError(f"Session '{session}' does not exist in status.json.")

        # Validate that the key exists within the session data
        if key not in status_data[session]:
            raise KeyError(f"Key '{key}' does not exist in session '{session}'.")

        # Update the specified key with the new value
        status_data[session][key] = value

        # Write the updated data back to status.json
        with open(status_file_location, 'w') as status_file:
            json.dump(status_data, status_file, indent=4)

        if self.debug:
            print(f"Updated status.json data: {status_data}")