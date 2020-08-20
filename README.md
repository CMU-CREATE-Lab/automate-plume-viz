# Setup the tool

SSH to the hal21 server and clone this repository. (This step is only for the CoCalc system administrator.)
```sh
ssh [USER_NAME]@hal21.andrew.cmu.edu
cd /projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/air-src/
git clone https://github.com/CMU-CREATE-Lab/automate-plume-viz.git
```
Change the permission of the folder so that the CoCalc system can read that. (This step is only for the CoCalc system administrator.)
```sh
chmod 777 automate-plume-viz
```
**Please do not edit the code on CoCalc if you want to use the code for your own project (see the next section).** For co-workers on this project, go to the [CoCalc project](https://cocalc.createlab.org:8443/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/files/air-src/automate-plume-viz/). You will see a list of files. The main script of this project is "automate_plume_viz.py" and you need to run it using the [terminal in the CoCalc system](https://cocalc.createlab.org:8443/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/files/air-src/automate-plume-viz/terminal.term?session=default). You can SSH to the hal21 server and use the Vim editor to write code. Or, another way is to go to the [CoCalc page that has the code](https://cocalc.createlab.org:8443/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/files/air-src/automate-plume-viz/automate_plume_viz.py?session=default), and edit the code using the CoCalc interface.

To begin the pipeline, run the following command to generate the EarthTime layers. This will create several files in the "data/" folder. You will need to open the "earth_time_layer.csv" file and copy the rows to the "[DAVOS2019 EarthTime Waypoints and CSV Layers](https://docs.google.com/spreadsheets/d/1zbXFtyevXqfZolxVPNhojZn7y_zxofbe_4UxYmdXp8k/edit#gid=870361385)" Google document. Ask Randy Sargent or Paul Dille for the permission to edit this file, and make sure you understand what each column means. 
```sh
python automate_plume_viz.py genetate_earthtime_data
```
Next, run the hysplit simulation and generate the particle files. By default, the script uses 4 workers in parallel. Make sure that you ask Randy Sargent about whether the CoCalc server is OK before running this command. Depending on the server condition, you may need to reduce the number of workers. This step uses a lot of CPU resources and will take a very long time (hours and days). Use the provided shell script "bg.sh" to run the code at the background, or use CoCalc interface to run the code, so that the program will not stop in the middle when you exit the terminal.
```sh
sh bg.sh python automate_plume_viz.py run_hysplit
```
Then, call the thumbnail server to process the video frames. By default, the script uses 4 workers in parallel. Make sure that you ask Paul Dille about whether the thumbnail server is OK before running this command. Depending on the server condition, you may need to reduce the number of workers. This step uses a lot of CPU resources and will take a very long time (hours and days). Notice that if you forget to copy and paste the EarthTime layers, this step will fail.
```sh
sh bg.sh python automate_plume_viz.py download_video_frames
```
Next, rename the downloaded video frames based on epochtime.
```sh
sh bg.sh python automate_plume_viz.py rename_video_frames
```
Then, create all videos in the "data/rgb/" folder. This step requires [opencv](https://github.com/skvark/opencv-python) and [ffmpeg](https://github.com/FFmpeg/FFmpeg) packages (ask the CoCalc system administrator to install these packages). Notice that the code will skip the dates that already have corresponding video files. To re-generate the video, you need to delete the video files.
```sh
sh bg.sh python automate_plume_viz.py create_all_videos
```
After creating the videos, the videos and other related files will be stored in "automate-plume-viz/data/rgb/" and you need to copy the videos to a place that has public access, by using the following command:
```sh
cp /projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/air-src/automate-plume-viz/data/rgb/*/*.mp4 /projects/cocalc-www.createlab.org/pardumps/video/
```
Finally, generate the json file for the [front-end plume visualization website](https://github.com/CMU-CREATE-Lab/plume-viz-website). You need to copy and paste the "data/plume_viz.json" file to the front-end website.
```sh
python automate_plume_viz.py generate_plume_viz_json
```
If you wish to run all of the steps at the background, use the following command:
```sh
sh bg.sh python automate_plume_viz.py pipeline
```
To add more dates in the pipeline, edit the genetate_earthtime_data() function in the "automate_plume_viz.py" file.

# For your application

**DO NOT edit the "automate_plume_viz.py" code or others in this project directly.** To use this code for your application, you need to:
- Go to [the "air-src" folder on CoCalc](https://cocalc.createlab.org:8443/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/files/air-src/?session=default) and create a new folder for your project (feel free to copy the code in this repository for your use)
- Search and read every "IMPORTANT" tag in the "automate_plume_viz.py" file
- Make sure that the share urls you generated have unique identifiers in the EarthTime layers by changing the "prefix" option for the "generate_metadata()" function 
- If you do not need smell reports in your visualization, change the "add_smell" option for the "generate_metadata()" function to False
- Change pollution sources in the "run_hysplit()" function

Before running large tasks that will take a long time on the CoCalc system, make sure that you notify and talk to system administrators.
