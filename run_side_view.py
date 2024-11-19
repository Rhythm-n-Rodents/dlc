import argparse
import os, sys
import socket
from datetime import datetime
import pdb

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'DeepWhiskerCuts'))
if module_path not in sys.path:
    sys.path.append(module_path)

from DeepWhiskerCuts.lib.ProgressManager import ExperimentManager
from DeepWhiskerCuts.lib.pipeline import *
from DeepWhiskerCuts.setting.setting import this_computer


#################################################################
# APP CONSTANTS (DEFAULT)
debug=True
base_input_location='/net/dk-server/'
base_output_location='/net/dk-server/'
#################################################################
def log_message(log_file, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} - {message}\n")


def capture_args():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process some data.')
    parser.add_argument('--host', type=str, required=True, help='source host')
    parser.add_argument('--log', type=str, required=False, help='log location')
    parser.add_argument('--user', type=str, required=True, help='user name')
    
    # Parse arguments
    args = parser.parse_args()
    
    src_host = args.host
    log_file = args.log
    user_name = args.user

    return (src_host, log_file, user_name)


def displayParam(log_file, user_name, input_location, output_location):
    sep='-' * 40
    log_message(log_file, sep)
    compute_host = socket.gethostname()
    param_info = (
        "USING THE FOLLOWING PARAMETERS:\n"
        f"\t\tRUN AS USER:\t {user_name}\n"
        f"\t\tCOMPUTE HOST:\t {compute_host}\n"
        f"\t\tINPUT FOLDER:\t '{input_location}'\n"
        f"\t\tOUTPUT FOLDER:\t '{output_location}'"
    )
    
    
    print(param_info)
    log_message(log_file, param_info)
    log_message(log_file, sep)


def process_data():
    side_folder = r'D:\Sidevideos\ar51_1vib_lighton\2024_05_30_ 172510'
    print(side_folder)
    #processs_side_view_data(side_folder)
    #make_movie_for_all_trials(side_folder,parallel=False,ncores=4)
    # analyze_side_view_video(side_folder)
    # extract_eye_videos(side_folder,'DLC_resnet50_SideviewLeft_Feb2022Feb8shuffle1_271000')
    # analyze_eye_video(side_folder)



def main():
    src_host, log_file, user_name = capture_args()
    
    print(f"Processing data for host: {src_host}")
    print(f"log location: {log_file}")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    log_message(log_file, f"Processing data for host: {src_host}")
    log_message(log_file, "Starting side view processing (python)")

    input_location = os.path.join(base_input_location, user_name, 'eyemovies')

    #Note: cam_location is also expected folder name
    match src_host:
        case "gyri":
            cam_location = "Rightcam2"
            
        case "protocerebrum-dk":
            cam_location = "Leftcam2"

        case "B6QTE70":
            cam_location = "Rightcam1"

        case "88QP74G":
            cam_location = "Leftcam1"

        case _:
            print("unrecognized src host; exiting")
            exit()

    input_location = os.path.join(input_location, cam_location)
    os.makedirs(os.path.dirname(input_location), exist_ok=True)

    output_location = os.path.join(base_output_location, user_name)
    os.makedirs(os.path.dirname(output_location), exist_ok=True)

    displayParam(log_file, user_name, input_location, output_location)


if __name__ == "__main__":
    main()
