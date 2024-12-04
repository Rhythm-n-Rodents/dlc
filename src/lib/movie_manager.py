import cv2 
from pathlib import Path
import re
from src.lib.utilities import get_scratch_dir, move_files_in_background, get_nworkers, run_commands_concurrently
from moviepy import ImageSequenceClip
from concurrent.futures.process import ProcessPoolExecutor

class MovieManager:
    def __init__(self):
        pass

    def process_img_recordings(self, metadata_status):
        if self.debug:
            print(f'DEBUG: MovieManager::process_img_recordings')
        if self.use_scratch:
            scratch_tmp = get_scratch_dir()
            self.fileLogger.logevent(f"STAGING FOLDER: {scratch_tmp}".ljust(20))
        
        for folder, folders_and_last_task in metadata_status.items():
            self.fileLogger.logevent(f"Process img recordings {folder} - {folders_and_last_task}.".ljust(20))
            
            #TASK GROUPING FOR SUBFOLDERS / SESSIONS
            for session, status in folders_and_last_task.items():
                last_task = status[1]
                files_cnt = status[0]
                input = Path(self.base_input_location, folder, session)
                final_output = input
                SCRATCH = Path(scratch_tmp, 'pipeline_behavior', folder, session, 'img_recordings')

                if last_task == 'create_json_manifest' or self.task == 'movie_creation':
                
                    SCRATCH.mkdir(parents=True, exist_ok=True)
                    debug = self.debug
                    files_cnt = files_cnt
                    
                    #OUTPUT WILL BE .avi,.mp4 FOR ALL SUBFOLDERS, STORED ON SCRATCH
                    self.make_movie_for_all_trials(input, SCRATCH, files_cnt, debug)

                    meta_data_filename = Path(final_output, "meta-data.json")
                    self.fileLogger.update_individual_json_manifest(meta_data_filename, 'movie_creation')
                else:
                    print(f'SKIPPING {folder}; MOVIES ALREADY CREATED')
                    
                if self.task == 'movie_creation':
                    print(f'MOVING PREVIOUSLY-CREATED MOVIES FROM {SCRATCH} TO FINAL OUTPUT FOLDER: {final_output}')
                    move_files_in_background('.avi', SCRATCH, final_output, self.move_or_copy_to_final_output, self.debug)
                    move_files_in_background('.mp4', SCRATCH, final_output, self.move_or_copy_to_final_output, self.debug)


    def make_movie_for_all_trials(self, input: Path, SCRATCH: Path, files_cnt: int, debug: bool):
        '''
        Processes all trial folders and creates movies for each trial.
        '''
        if debug:
            print(f'DEBUG: MovieManager::make_movie_for_all_trials')

        for trial in range(files_cnt):
            img_trial_folder = Path(input, str(trial))
            self.make_and_convert_movie(img_trial_folder, SCRATCH, debug)
    

    def make_and_convert_movie(self, img_trial_folder: Path, SCRATCH: Path, debug: bool):
        '''
        Creates AVI and MP4 movies from images in the specified trial folder.
        '''
        avi_filename = Path(img_trial_folder).name
        staging_location = str(Path(SCRATCH, avi_filename + '.avi'))
        if not Path(staging_location).is_file():
            self.concat_images_to_movie(img_trial_folder, staging_location, debug)
        

    def concat_images_to_movie(self, image_dir: str, avi_name: str, debug: bool):
        '''
        Concatenates images into an .avi and .mp4 files
        '''
        if not debug:
            workers = get_nworkers()
        else:
            workers = 1

        mp4_name = Path(avi_name).with_suffix('.mp4')

        image_extensions = ('.jpg', '.jpeg', '.JPG', '.JPEG')
        # Sort images by numeric value in the filename (USES NATURAL SORT)
        images = sorted(
            [str(f) for f in Path(image_dir).iterdir() if f.suffix.lower() in image_extensions],
            key=lambda x: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', x)]
        )

        if not images:
            self.fileLogger.logevent(f"No images found in {image_dir}".ljust(20))
            return
        else:
            if debug:
                print(f'Concatenating images from {image_dir} to {avi_name}')
        
        # Read the first image to get dimensions
        frame = cv2.imread(images[0])
        if frame is None:
            self.fileLogger.logevent(f"Unable to read image: {images[0]}".ljust(20))
            return
            
        # Use concurrent processing to read images
        with ProcessPoolExecutor(max_workers=workers) as executor:
            frames = list(executor.map(self.read_image_with_path, images))
        
        frames = [frame for _, frame in frames if frame is not None]

        video_info = [
            (str(avi_name), 'avi', 40, frames),
            (str(mp4_name), 'mp4', 40, frames)
        ]
        
        run_commands_concurrently(self.write_video, video_info, workers)


    def read_image_with_path(self, image_path: str):
        '''
        Read an image from the given path and return it with its path.
        Expected usage in parallel processing.
        '''
        return (image_path, cv2.imread(image_path))
    

    def write_video(self, video_info: tuple[str, str, int, list]) -> str:
        ''' 
        Write a video file from the given frames.
        '''
        output_filename, format_type, fps, frames = video_info
        
        # Create a clip from the image sequence
        clip = ImageSequenceClip(frames, fps=fps)
        
        # Set the codec based on format type
        if format_type == 'avi':
            moviepy_codec = 'rawvideo' 
        elif format_type == 'mp4':
            moviepy_codec = 'libx264'  # Use 'libx264' codec for MP4
        else:
            raise ValueError(f"Unsupported format type: {format_type}")

        clip.write_videofile(output_filename, codec=moviepy_codec)
        return f"Video creation completed: {output_filename}"