This repository hosts the code for generating videos that show the visualizations of [smell reports](https://smellpgh.org/) and forward dispersion simulation (using the [HYSPLIT model](https://www.ready.noaa.gov/HYSPLIT.php)). For getting wind information, you may want to check [WindNinja](https://www.firelab.org/project/windninja).

# Setup the tool
Go to [the "air-src" folder on CoCalc](https://aircocalc.createlab.org:8443/projects/13e67e6d-d6b5-42f2-99ff-cda6431e4c2f/files/air-src/?session=default) and create a new folder for your project. Or you can SSH to the hal50 server and clone this repository.
```sh
ssh [USER_NAME]@hal50.pc.cs.cmu.edu
cd [YOUR_PATH]
git clone https://github.com/CMU-CREATE-Lab/automate-plume-viz.git
```
Change the permission of the folder so that the CoCalc system can read that.
```sh
chmod 777 automate-plume-viz
```
You can SSH to the hal50 server and use the Vim editor to write code. Or you can edit the code using the CoCalc interface. Make sure you run the following first to activate the conda environment on the hal50 server:
```sh
. /projects/anaconda3/bin/activate
```
**Before you start, read every "IMPORTANT" tag in the "main.py" file carefully.** You need to specify several variables in that file. For example, the path to store files, the url to access files, the dates that you want to process, the information about the pollution source, the location of the map, and parameters of the hysplit model.

To begin the pipeline, run the following command to generate the EarthTime layers. This will create several files in the "data/" folder. You will need to open the "earth_time_layer.csv" file and copy the rows to the "[DAVOS2019 EarthTime Waypoints and CSV Layers](https://docs.google.com/spreadsheets/d/1zbXFtyevXqfZolxVPNhojZn7y_zxofbe_4UxYmdXp8k/edit#gid=870361385)" Google document. Ask Randy Sargent or Paul Dille for the permission to edit this file, and make sure you understand what each column means. 
```sh
python main.py genetate_earthtime_data
```
Next, run the hysplit simulation and generate the particle files. By default, the script uses 4 workers in parallel. Make sure that you ask Randy Sargent about whether the CoCalc server is OK before running this command. Depending on the server condition, you may need to reduce the number of workers. This step uses a lot of CPU resources and takes a very long time (hours and days). This command will run a [screen](https://www.gnu.org/software/screen/manual/html_node/index.html) at the background.
```sh
sh bg.sh python main.py run_hysplit
```
The above command uses the provided shell script "bg.sh" to run the code at the background, using [Screen](https://www.gnu.org/software/screen/manual/html_node/index.html). You can also use CoCalc interface to run the code, so that the program will not stop in the middle when you exit the terminal. If you use the provided shell script, here are some tips for the Screen command:
```sh
# List currently running screen names
screen -ls

# Go into a screen
screen -x [NAME_FROM_ABOVE_COMMAND] (e.g. sudo screen -x 33186.download_videos)
# Inside the screen, use CTRL+C to terminate the screen
# Or use CTRL+A+D to detach the screen and send it to the background

# Terminate all screens
screen -X quit

# Keep looking at the screen log
tail -f screenlog.0
```
Then, call the thumbnail server to process the video frames. By default, the script uses 4 workers in parallel. Make sure that you ask Paul Dille about whether the thumbnail server is OK before running this command. Depending on the server condition, you may need to reduce the number of workers. This step uses a lot of CPU resources and takes a very long time (hours and days). Notice that if you forget to copy and paste the EarthTime layers, this step will fail.
```sh
sh bg.sh python main.py download_video_frames
```
Then, create all videos in the "data/rgb/" folder. This step requires [opencv](https://github.com/skvark/opencv-python) and [ffmpeg](https://github.com/FFmpeg/FFmpeg) packages (ask the CoCalc system administrator to install these packages). Notice that the code will skip the dates that already have corresponding video files. To re-generate the video, you need to delete the video files in the "data/rgb/" folder.
```sh
sh bg.sh python main.py create_all_videos
```
To access the videos, go to "https://aircocalc-www.createlab.org/pardumps/" and select the folders or files. Finally, generate the json file for the [front-end plume visualization website](https://github.com/CMU-CREATE-Lab/plume-viz-website). You need to copy and paste the "data/plume_viz.json" file to the front-end website.
```sh
python main.py generate_plume_viz_json
```
**Before running large tasks that will take a long time on the CoCalc system, make sure that you notify and talk to system administrators.**

# About the main project

**If you work on your own project but not the CREATE Lab's plume visualization project, DO NOT use the following instructions.**

The main project folder is "/projects/earthtime/air-src/automate-plume-viz" on the hal50 server. For co-workers on the CREATE Lab's plume visualization project, go to the [CoCalc project](https://aircocalc.createlab.org:8443/projects/13e67e6d-d6b5-42f2-99ff-cda6431e4c2f/files/air-src/automate-plume-viz/). You will see a list of files. The main script of this project is "automate_plume_viz.py" and you need to run it using the [terminal in the CoCalc system](https://aircocalc.createlab.org:8443/projects/13e67e6d-d6b5-42f2-99ff-cda6431e4c2f/files/air-src/automate-plume-viz/terminal.term?session=default).
