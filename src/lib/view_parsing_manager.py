import os
from pathlib import Path
import time
import pandas as pd
import deeplabcut
import re
import numpy as np
import math
import cv2 
from PIL import Image, ImageEnhance
import lib.image_util as image_util
from lib.utilities import get_scratch_dir, move_files_in_background
import settings.dlc_setting as dlc_config

from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows


from settings.settings import this_computer


class ViewParsingManager:
    def __init__(self):
        super().__init__()

        self.side_view_shuffle = dlc_config.side_view_shuffle
        self.whisker_shuffle = dlc_config.whisker_shuffle
        self.eye_shuffle = dlc_config.eye_shuffle

        self.side_view_config_file = dlc_config.side_view_config_file
        self.eye_config_file = dlc_config.eye_config_file


    def process_top_view_videos(self, metadata_status: dict):
        '''
        #ALSO CONTINUES TO PARSE LEFT/RIGHT FROM TOP VIEW VIDEO
        #prev: pipeline::analyze_top_view_video
        '''
        if self.debug:
            print(f'DEBUG: ViewParsingManager::process_top_view_videos')

        top_view_config = Path(dlc_config.linux_dlc_folder, dlc_config.top_view_config_file)
        
        self.fileLogger.logevent(f"USING TRAINING MODEL: {top_view_config}".ljust(20))

        if self.use_scratch:
            scratch_tmp = get_scratch_dir()
        
        for folder, subfolders in metadata_status.items():
            self.fileLogger.logevent(f"Posture extraction from top view {folder} - {subfolders}.".ljust(20))
            
            #TASK GROUPING FOR SUBFOLDERS / SESSIONS
            for session, status in subfolders.items():
                files_cnt = status[0]
                final_output = Path(self.base_output_location, folder, session)
                meta_data_filename = Path(final_output, "meta-data.json")
                SCRATCH = Path(scratch_tmp, 'pipeline_behavior', folder, session, 'img_recordings')

                # GET ALL .avi FILES MATCHING {number}.avi
                top_movie_files = [
                    file for file in SCRATCH.glob("*.avi") 
                    if re.match(r'^\d+\.avi$', file.name)
                ]
                #SECONDARY STORAGE LOCATION (IF ALREADY MOVED)
                top_movie_files_final = [
                    file for file in final_output.glob("*.avi") 
                    if re.match(r'^\d+\.avi$', file.name)
                ]
                if len(top_movie_files) > 0 and len(top_movie_files) == files_cnt:
                    print(f'.avi FILE COUNT MATCHES EXPECTED COUNT')
                    
                    self.analyze_all_videos(top_movie_files, top_view_config, shuffle=dlc_config.top_shuffle)
                    self.fileLogger.update_individual_json_manifest(meta_data_filename, 'analyze_movies')
                    
                    self.split_left_and_right_from_top_video(SCRATCH)
                    self.fileLogger.update_individual_json_manifest(meta_data_filename, 'split_top_left_right')

                    self.analyze_left_video(SCRATCH)
                    self.fileLogger.update_individual_json_manifest(meta_data_filename, 'analyze_left_video')

                    self.analyze_right_video(SCRATCH)
                    self.fileLogger.update_individual_json_manifest(meta_data_filename, 'analyze_right_video')

                    self.writeFrameData_from_top_video(SCRATCH)
                    self.fileLogger.update_individual_json_manifest(meta_data_filename, 'writeFrameData_from_top_video')

                    if self.debug:
                        print(f'MOVING ANALYSIS FILES FROM {SCRATCH} TO {final_output}')
                    move_files_in_background('.avi', SCRATCH, final_output, self.debug)
                    move_files_in_background('.mp4', SCRATCH, final_output, self.debug)
                    move_files_in_background('.csv', SCRATCH, final_output, self.debug)
                    move_files_in_background('.pickle', SCRATCH, final_output, self.debug)
                    move_files_in_background('.h5', SCRATCH, final_output, self.debug)
                    status = (session, 'processed', True)

                else:
                    print(f'INCORRECT .avi FILE COUNT: EXPECTED={len(top_movie_files)} ACTUAL={files_cnt}')
                    print(f'MOVIE FILE COUNT ON STORAGE ({SCRATCH}): {len(top_movie_files)}, meta-data FOLDER COUNT: {files_cnt}')
                    print(f'TRYING final_output ON SERVER: {final_output}')

                    print(f'SKIPPING {folder}, {session}')
                
                if status[1] == 'processed':
                    self.fileLogger.update_metadata_status_file(Path(self.base_input_location, folder), status)
                else:
                    print('NO UPDATES TO status.json')

        if self.debug:
            print('Finished all top view steps.')
    

    def process_side_view_videos(self, metadata_status: dict):
        '''
        prev: pipeline::processs_side_view_data
        '''
        if self.debug:
            print(f'DEBUG: ViewParsingManager::process_side_view_videos')


        side_view_config = Path(dlc_config.linux_dlc_folder, dlc_config.side_view_config_file)
        
        self.fileLogger.logevent(f"USING TRAINING MODEL: {side_view_config}".ljust(20))

        if self.use_scratch:
            scratch_tmp = get_scratch_dir()

        for folder, subfolders in metadata_status.items():
            self.fileLogger.logevent(f"Process side view {folder} - {subfolders}.".ljust(20))

            #TASK GROUPING FOR SUBFOLDERS / SESSIONS
            for session, status in subfolders.items():
                files_cnt = status[0]
                final_output = Path(self.base_output_location, folder, session)
                meta_data_filename = Path(final_output, "meta-data.json")
                SCRATCH = Path(scratch_tmp, 'pipeline_behavior', folder, session, 'img_recordings')

        # make_movie_for_all_trials(data_path,parallel=False,ncores=4)
        # analyze_side_view_video(data_path)
        # extract_eye_videos(data_path,'DLC_resnet50_SideviewLeft_Feb2022Feb8shuffle1_271000')
        # analyze_eye_video(data_path)


    def analyze_all_videos(self, video_files, training_model, shuffle: int = 3):
        ''' prev. analyze_videos(videos,config_type,shuffle=3)
            EXPECTED OUTPUT {FROM DEEPLABCUT}: filtered csv file per session
        '''

        if self.debug:
            print(f'DEBUG: ViewParsingManager::analyze_all_videos')

        self.fileLogger.logevent(f"analyze_all_videos: MODEL:{training_model}, {shuffle=}.".ljust(20))

        for individual_video_file in video_files:
            if self.debug:
                print(f'DEBUG: Analyzing individual video file & filtering predictions: {individual_video_file}')
            deeplabcut.analyze_videos(training_model, [str(individual_video_file)], shuffle=shuffle, save_as_csv=True)
            deeplabcut.filterpredictions(training_model, [str(individual_video_file)], shuffle=shuffle, save_as_csv=True)


    def analyze_left_video(self, data_path, shuffle: int = dlc_config.left_shuffle):
        left_videos = [os.path.join(data_path,f) for f in os.listdir(data_path) if f.startswith('Mask')  and  f.endswith('.avi')  ] 
        self.analyze_all_videos(left_videos, dlc_config.whisker_config_file, shuffle)


    def analyze_right_video(self, data_path, shuffle: int = dlc_config.right_shuffle):
        right_videos = [os.path.join(data_path,f) for f in os.listdir(data_path) if f.startswith('Mirror') and  f.endswith('.avi')  ] 
        self.analyze_all_videos(right_videos, dlc_config.whisker_config_file, shuffle)


    def split_left_and_right_from_top_video(self, data_path: Path):
        ''' prev. split_left_and_right_from_top_video(data_path)'''

        if self.debug:
            print(f'DEBUG: ViewParsingManager::split_left_and_right_from_top_video')

        #not really text files (top) - rename
        text_files = [os.path.join(data_path,f) for f in os.listdir(data_path) if f.endswith('.avi') and not f.endswith('L.avi') and not f.endswith('R.avi') and not f.endswith('videopoints.avi') and not f.endswith('videopoints.avi')]

        for trial in range(len(text_files)):
            t = time.time()
            df, head_angle, interbead_distance, movie_name = self.readDLCfiles(data_path, trial)
            text = os.path.basename(movie_name)
            good_frames = self.find_good_frames(0.7, 5, 200, df, interbead_distance)
            
            self.savemovies_LR(movie_name, head_angle, df, good_frames, self.contrastfactor) 
            elapsed = time.time() - t 
            video_name = (os.path.join(os.path.dirname(movie_name),text.split('DLC')[0]+".avi"));
            
            print('Trial=', video_name, 'Elapsed', elapsed)


    def readDLCfiles(self, data_path: Path, trial: int):  
        smoothingwin = 5
        Xfiles = [
            os.path.join(data_path, i)
            for i in os.listdir(data_path)
            if (match := re.match(r'(\d+)videoDLC.*filtered\.csv', i)) and int(match.group(1)) == trial
        ]
        trial_num_len = len(str(trial)) 
        print(data_path)
        Xfiles = [
            os.path.join(data_path, i)
            for i in os.listdir(data_path)
            if i[:trial_num_len] == str(trial)
            and re.match(r'\d+DLC', i)
            and 'filtered.csv' in i
            and i[trial_num_len].isalpha()
        ]
        
        if len(Xfiles) != 1:
            print(f'ERROR: Expected at least one filtered.csv file in {data_path}')
            return None, None, None, None
        
        filename = Xfiles[0]
        
        df = pd.read_csv(filename, header=2, usecols = ['x','y', 'likelihood', 'x.1', 'y.1', 'likelihood.1'])
        df.columns = ['Nosex', 'Nosey', 'Noselikelihood', 'Snoutx1', 'Snouty1', 'Snoutlikelihood']
        
        x1 = self.smooth_data_convolve_my_average(df.Nosex, smoothingwin)
        y1 = self.smooth_data_convolve_my_average(df.Nosey, smoothingwin)
        x2 = self.smooth_data_convolve_my_average(df.Snoutx1, smoothingwin)
        y2 = self.smooth_data_convolve_my_average(df.Snouty1, smoothingwin)
        head_angles = [math.atan2(-(y1[i]-y2[i]),-(x1[i]-x2[i]))  for i in range(len(df.Snoutlikelihood))] # define the angle of the head
        inter_bead_distance = [math.sqrt((x2[i] - x1[i])**2 + (y2[i] - y1[i])**2)  for i in range(len(df.Snoutlikelihood))]# define the distance between beads  
        head_angles = pd.Series(head_angles)

        return df, head_angles, inter_bead_distance, filename
        

    def smooth_data_convolve_my_average(self, arr, span):
        re = np.convolve(arr, np.ones(span * 2 + 1) / (span * 2 + 1), mode="same")
        re[0] = np.average(arr[:span])
        for i in range(1, span + 1):
            re[i] = np.average(arr[:i + span])
            re[-i] = np.average(arr[-i - span:])
        return re
    

    def find_good_frames(self, Minliklihood, mindist, maxdist, df, Distance):
        Good_Frames = [0 if df.Noselikelihood[i] <Minliklihood or df.Snoutlikelihood[i] <Minliklihood or Distance[i]<mindist or Distance[i]>maxdist else 1 for i in range(len(df.Snoutlikelihood))]
        a=pd.Series(Good_Frames)
        a[a==0] = np.nan
        return a
    

    def savemovies_LR(self, movie_name: str, head_angle, df, good_frames, factor): 
        if self.debug:
            print(f'DEBUG: ViewParsingManager::savemovies_LR')
        
        text = os.path.basename(movie_name)
        base_name = os.path.join(os.path.dirname(movie_name), text.split('DLC')[0])
        video_name = f"{base_name}.avi"
        video_nameR = f"Mirror{base_name}R.avi"
        video_nameL = f"Mask{base_name}L.avi"
        self.process_and_split_video(video_name, video_nameR, good_frames, head_angle, df, factor, 315, 630, faceshift=60, flip=True)
        self.process_and_split_video(video_name, video_nameL, good_frames, head_angle, df, factor, 0, 315, faceshift=80)


    def process_and_split_video(self, input_name: str, output_name: str, good_frames, head_angle, df, factor, start_index, end_index, faceshift=60, flip=False):
        if self.debug:
            if flip:
                print(f'DEBUG: ViewParsingManager::process_and_split_video - {input_name}, right')
            else:
                print(f'DEBUG: ViewParsingManager::process_and_split_video - {input_name}, left')
        
        cap = cv2.VideoCapture(input_name)
        video = cv2.VideoWriter(output_name, 0, 40, (315,700))
        
        i=0
        while(cap.isOpened()):
            ret, frame = cap.read()
            if frame is None:
                break
            if ret == True:
                i+=1
            if len(good_frames)>i:
                if good_frames[i-1]==1:
                    color_coverted = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    image = Image.fromarray(color_coverted)
                    rotated = image.rotate((math.degrees(head_angle[i])-90+180), expand=True)
                    rotated = np.array(rotated) 
                    rotated = rotated[:, :, ::-1].copy() 
                    cropped=  self.crop_rotated(rotated, frame, head_angle, i, df)
                    cropped_image = cropped[0:700, start_index+faceshift:end_index+faceshift]
                    if flip:
                        frame2 = cv2.flip(cropped_image, 1)
                    else:
                        frame2 = cropped_image
                    frame2 = image_util.Mask(frame2,60)
                    frame2 = Image.fromarray(frame2)
                    frame2 = frame2.convert("RGB")
                    enhancer = ImageEnhance.Contrast(frame2)
                    enhanced = enhancer.enhance(factor)
                    video.write(np.array(enhanced))
            else:
                break 

        else:
            print("Error opening the video file")
        video.release()


    def crop_rotated(self, rotated: np.ndarray, frame: np.ndarray, Angle, i: int, df: pd.DataFrame):

        img = Image.fromarray(rotated, 'RGB')
        Newrotated=np.uint8(self.add_margin(img, 400, 400, 400, 400, (0,0,0)))
        Alpharad = math.radians(math.degrees(Angle[i])-90+180)
        P = [df.Nosey[i] ,df.Nosex[i]]
        c, s = np.cos(Alpharad),np.sin(Alpharad)
        RotMatrix = np.array(((c, -s), (s, c)))
        ImCenterA = np.array(frame.shape[0:2])/2       # Center of the main image
        ImCenterB = np.array(Newrotated.shape[0:2])/2  # Center of the transformed image
        RotatedP =RotMatrix.dot(P-ImCenterA)+ImCenterB
        midpoint= 350
        sizetotal = 700
        ratsiosize = 1.1
        y= int(RotatedP[0]-midpoint)
        x= int(RotatedP[1]-midpoint*ratsiosize)
        h = sizetotal
        w = int(sizetotal*ratsiosize)
        crop_img = Newrotated[y:y+h, x:x+w]
        return crop_img
    

    def add_margin(self, pil_img, top, right, bottom, left, color):
        width, height = pil_img.size
        new_width = width + right + left
        new_height = height + top + bottom
        result = Image.new(pil_img.mode, (new_width, new_height), color)
        result.paste(pil_img, (left, top))
        return result
    

    def writeFrameData_from_top_video(self, data_path):
        if self.debug:
            print(f'DEBUG: ViewParsingManager::writeFrameData_from_top_video')

        contrastfactor=self.contrastfactor #TODO: remove variable if not used
        text_files = [os.path.join(data_path,f) for f in os.listdir(data_path) if f.endswith('.avi') and not f.endswith('L.avi') and not f.endswith('R.avi') and not f.endswith('videopoints.avi') and not f.endswith('videopoints.avi')]
        for trial in range(len(text_files)):
            t =time.time()
            df, head_angle,interbead_distance,movie_name=self.readDLCfiles(data_path, trial)
            text = os.path.basename(movie_name)
            good_frames = self.find_good_frames(0.7,5,200,df,interbead_distance)
            self.writeFrameData(data_path,text,good_frames,df,head_angle)
            #savemovies_LR(movie_name,head_angle,df,good_frames,".avi",contrastfactor) 
            elapsed = time.time() - t 
            video_name = (os.path.join(os.path.dirname(movie_name),text.split('DLC')[0]+".avi"))
            print('Trial=',video_name,'Elapsed',elapsed)


    def writeFrameData(self, data_path, text, Good_Frames, df, Angle):
        #frame_data_path = os.path.join(data_path,text.split('DLC')[0]+'FrameData.xlsx');
        #good_frame_id = np.where(np.array(Good_Frames) == 1)[0]
        #results=pd.DataFrame({"goodframes":good_frame_id, "Angle":Angle[Good_Frames==1], "Nosex":df.Nosex[Good_Frames==1],"Nosey":df.Nosey[Good_Frames==1],"Snoutx":df.Snoutx1[Good_Frames==1],"Snouty":df.Snouty1[Good_Frames==1]})                     
        #results.to_csv(frame_data_path)

        frame_data_path = os.path.join(data_path,text.split('DLC')[0]+'FrameData.xlsx');
        pos = np.where(np.array(Good_Frames) == 1)[0]
        results=pd.DataFrame({"goodframes":pos, "Angle":Angle[Good_Frames==1], "Nosex":df.Nosex[Good_Frames==1]\
        ,"Nosey":df.Nosey[Good_Frames==1],"Snoutx":df.Snoutx1[Good_Frames==1],"Snouty":df.Snouty1[Good_Frames==1]})                     
        # print(results)

        # Specify a writer
        wb_target = Workbook()
        writer = wb_target.active

        rows = dataframe_to_rows(results, index=False)
        for r_idx, row in enumerate(rows, 1):
            for c_idx, value in enumerate(row, 1):
                writer.cell(row=r_idx, column=c_idx, value=value)
        wb_target.save(frame_data_path) 


    def find_good_frames(self, Minliklihood, mindist, maxdist, df, Distance):
        Good_Frames = [0 if df.Noselikelihood[i] <Minliklihood or df.Snoutlikelihood[i] <Minliklihood or Distance[i]<mindist or Distance[i]>maxdist else 1 for i in range(len(df.Snoutlikelihood))]
        a=pd.Series(Good_Frames)
        a[a==0] = np.nan
        return a