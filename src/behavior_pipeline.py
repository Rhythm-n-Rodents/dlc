import getpass
import subprocess
from pathlib import Path
from lib.movie_manager import MovieManager
from lib.view_parsing_manager import ViewParsingManager
from lib.utilities import get_scratch_dir, delete_in_background
from lib.file_logger import FileLogger


class Pipeline(MovieManager, ViewParsingManager):
    """
    This is the main class that handles the behavior pipeline 
    """


    def __init__(self, base_input_location, base_output_location, perspective, move_or_copy_to_final_output, src_host, compute_host, task, log_file, user_name, contrastfactor, debug=False):
        '''Setting up the pipeline and the processing configurations'''
        super().__init__()
        self.base_input_location=Path(base_input_location)
        self.base_output_location=Path(base_output_location)
        self.perspective=str(perspective)
        self.move_or_copy_to_final_output=str(move_or_copy_to_final_output)
        self.src_host=src_host
        self.compute_host=compute_host
        self.task=task
        self.user_name=user_name
        self.contrastfactor=contrastfactor
        self.debug = debug
        self.log_file=log_file
        self.fileLogger = FileLogger(log_file, self.debug)
        self.report_status()
        self.use_scratch = True # set to True to use scratch space (defined in - utilities::get_scratch_dir)


    def report_status(self):
        print()
        print(f"LOG FILE LOCATION: {self.log_file=}".ljust(20))
        print()
        self.fileLogger.logevent("RUNNING WITH THE FOLLOWING SETTINGS:")
        
        self.fileLogger.logevent(f"whoami - {getpass.getuser()}".ljust(20))
        self.fileLogger.logevent(f"{self.src_host=}".ljust(20))
        self.fileLogger.logevent(f"{self.compute_host=}".ljust(20))
        self.fileLogger.logevent(f"{self.task=}".ljust(20))
        self.fileLogger.logevent(f"{self.user_name=}".ljust(20))
        self.fileLogger.logevent(f"{self.debug=}".ljust(20))
        

    def all(self):
        
        if self.base_input_location.is_dir():
            self.fileLogger.logevent(f"INPUT FOLDER: {self.base_input_location}".ljust(20))
        else:
            self.fileLogger.logevent(f"INPUT FOLDER DOES NOT EXIST; EXITING: {self.base_input_location}".ljust(20))
            exit()

        self.base_output_location.mkdir(parents=True, exist_ok=True)
        self.fileLogger.logevent(f"FINAL OUTPUT FOLDER: {self.base_output_location}".ljust(20))
    
        #CHECK COUNT OF OUTSTANDING JOBS IN base_input_location
        metadata_status = self.fileLogger.read_metadata_status_files(self.base_input_location, self.debug)
        total_count = sum(len(subfolder_dict) for subfolder_dict in metadata_status.values())
        self.fileLogger.logevent(f"There are {total_count} outstanding job(s) to process.".ljust(20))
        
        self.process_img_recordings(metadata_status)
        
        if self.perspective == 'top':
            self.process_top_view_videos(metadata_status)
        elif self.perspective == 'side':
            self.process_side_view_videos(metadata_status)
        else:
            print(f'Invalid perspective: {self.perspective}')

        #CLEAN UP staging_output
        # if SCRATCH.exists():
        #     print(f'Removing {SCRATCH}')
        #     delete_in_background(SCRATCH)


    def movie_creation(self):
        if self.debug:
            print(f'DEBUG: Start movie_creation')
        if self.base_input_location.is_dir():
            self.fileLogger.logevent(f"INPUT FOLDER: {self.base_input_location}".ljust(20))
        else:
            self.fileLogger.logevent(f"INPUT FOLDER DOES NOT EXIST; EXITING: {self.base_input_location}".ljust(20))
            exit()
            
        self.base_output_location.mkdir(parents=True, exist_ok=True)
        self.fileLogger.logevent(f"FINAL OUTPUT FOLDER: {self.base_output_location}".ljust(20))
                                 
        #CHECK COUNT OF OUTSTANDING JOBS IN base_input_location
        metadata_status = self.fileLogger.read_metadata_status_files(self.base_input_location, self.debug)
        total_count = sum(len(subfolder_dict) for subfolder_dict in metadata_status.values())
        self.fileLogger.logevent(f"There are {total_count} outstanding job(s) to process.".ljust(20))
        
        self.process_img_recordings(metadata_status)

        if self.debug:
            print(f'DEBUG: End movie_creation')