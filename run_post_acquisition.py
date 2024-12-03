'''
This script is primary post-acquisition script for behavioral experiments

WORKFLOW:
    1. Windows workstation acquisition stores data locally
    2. Data backup tool is scheduled to move data to server (may also be performed manually)
    3. When data transfer has completed, local script is called on acquisition computer (C:\experiments_nwb\trigger.py)
        The trigger.py file does remote call to server to initiate processing of data (via SSH password-less login)
    4. Server script (post_acquire.sh) should be in user home folder and will parse processing request
        Arguments passed to script include user, host (source of data computer name), IP address
        The launching script (post_acquire.sh) on compute server will triage requests and call actual processing scripts
        Note: processing scripts are stored by default on computer server under /data/behavior folder and data storage location
        is mounted on same server
    5. Current script 'run_post_acquisition.py' is called from post_acquire.sh
        Note: Currently behavior pipeline is called (behavior_pipeline.py) for all tasks (default) however discreet tasks may be called individually

- python run_post_acquisition.py --host {hostname} --log {log_location} --user {username} --task {task} --debug={true | false}

EXAMPLE RUN (host='lil-whisker', log="/tmp/log/log.txt", user='drinehart'):
- python run_post_acquisition.py --host lil-whisker --log /tmp/log/log.txt --user drinehart --debug=false

Note: example does not include task as that feature is currently in development

N.B. When folders are processed, meta-data.json is created in each.  Steps for 'last_task' (key in meta-data.json) are:
'create_json_manifest' : Folder discovered but nothing done to folder. AVI/MP4 movies will be created
'movie_creation' : Movies created. Movies will be parsed for views
'analyze_movies' : Posture extraction based on DLC model; filtered results stored. Left/Right split from top videos next
'split_top_left_right' : Splits left and right videos from top view. analyze_left_video next
'analyze_left_video' : Extracts posture from left videos. Whisker_shuffle is applied. analyze_right_video next
'analyze_right_video' : Extracts posture from right videos. Whisker_shuffle is applied. writeFrameData_from_top_video next
'writeFrameData_from_top_video' : Need description

REQUIREMENTS:
-Needs rclone installed to move files from scratch to destination en masse

CONFIG NOTES:
-use_absolute_locations: if set to True, base_input_location and base_output_location will not be set based on username and source host
                            if you wish to modify src_host name(s) or corresponding folders, see auto_populate_data_location function
'''

#################################################################
# APP CONSTANTS (DEFAULT)
base_input_location='/net/dk-server/'
base_output_location='/net/dk-server/'

#GENERAL CONFIG
use_absolute_locations=False #sys will autopopulate based on user & src_host
contrastfactor=1.05 #used in splitting left and right from top view
move_or_copy_to_final_output = 'copy'
#################################################################

import argparse
import os
import sys, socket
from timeit import default_timer as timer
from pathlib import Path

PIPELINE_ROOT = Path("./src").absolute()
sys.path.append(PIPELINE_ROOT.as_posix())

from src.behavior_pipeline import Pipeline


def capture_args():
    parser = argparse.ArgumentParser(description='Process some data.')
    parser.add_argument('--host', type=str, required=True, help='source host')
    parser.add_argument('--log', type=str, required=False, help='log location')
    parser.add_argument('--user', type=str, required=False, help='user name')
    parser.add_argument(
        "--task",
        type=str,
        help="Enter the task you want to perform: \
                        all | movie_creation | NA",
        required=False,
        default="all",
    )
    parser.add_argument(
        "--debug", help="Enter true or false", required=False, default="false", type=str
    )
    args = parser.parse_args()
    
    src_host = args.host
    log_file = args.log
    if not log_file:
        log_file = Path(os.getcwd(), 'behavior_pipeline_log.txt')
    
    user_name = args.user
    if not user_name:
        user_name = os.getlogin()

    compute_host = socket.gethostname()
    task = str(args.task).strip().lower()
    if not task:
        task = 'all'

    debug = bool({"true": True, "false": False}[str(args.debug).lower()])

    return (src_host, compute_host, task, log_file, user_name, debug)


def auto_populate_data_location(base_location, user_name, src_host):
    location = os.path.join(base_location, user_name)

    #Note: cam_location is also expected folder name
    match src_host:
        case "lil-whisker":
            cam_location = 'Topviewmovies'
            perspective = 'top'
        case "gyri":
            cam_location = Path('eyemovies', 'Rightcam2')
            perspective = 'side'
        case _:
            print(f"unrecognized src host; using raw {base_location}")
            cam_location = ''
            
    location = Path(location, cam_location)
    
    return (location, perspective)


def main():
    src_host, compute_host, task, log_file, user_name, debug = capture_args()
    
    if not use_absolute_locations:
        input_location, perspective = auto_populate_data_location(base_input_location, user_name, src_host)
        output_location, perspective = auto_populate_data_location(base_output_location, user_name, src_host)
    else:
        input_location, perspective = base_input_location, user_name, src_host
        output_location, perspective = base_output_location, user_name, src_host

    pipeline = Pipeline(
        str(input_location),
        str(output_location),
        str(perspective),
        str(move_or_copy_to_final_output),
        src_host=src_host,
        compute_host=compute_host,
        task=task,
        user_name=user_name,
        contrastfactor=contrastfactor,
        debug=debug,
        log_file=log_file,
    )

    #FOR MANUAL PROCESSING OF SPECIFIC FUNCTIONALITY
    function_mapping = {
        "all": pipeline.all,
        "movie_creation": pipeline.movie_creation
    }

    start_time = timer()
    if task in function_mapping:
        print()
        print(f'START BEHAVIOR PIPELINE:{str(task)} @ PROCESS_HOST={compute_host}')
        function_mapping[task]()

    end_time = timer()
    total_elapsed_time = round((end_time - start_time), 2)
    if total_elapsed_time >= 3600:
        hours = total_elapsed_time // 3600
        minutes = (total_elapsed_time % 3600) // 60
        time_out_msg = f'took {int(hours)} hour(s) and {int(minutes)} minute(s).'
    else:
        time_out_msg = f'took {total_elapsed_time} seconds.'

    print(f"END {time_out_msg}")
    sep = "*" * 40 + "\n"
    pipeline.fileLogger.logevent(f"END {time_out_msg}\n{sep}")


if __name__ == "__main__":
    main()