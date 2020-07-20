SSH to the hal21 server and clone this repository
```sh
ssh [USER_NAME]@hal21.andrew.cmu.edu
cd /projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/air-src/
git clone https://github.com/CMU-CREATE-Lab/automate-plume-viz.git
```
Change the permission of the folder so that the CoCalc system can read that.
```sh
chmod 777 automate-plume-viz
```
Then, go to the [CoCalc project](https://cocalc.createlab.org:8443/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/files/air-src/automate-plume-viz/). You will see a list of files. The main script of this project is "automate_plume_viz.py" and you need to run it using the [terminal in the CoCalc system](https://cocalc.createlab.org:8443/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/files/air-src/automate-plume-viz/terminal.term?session=default) with the following command:
```sh
python automate_plume_viz.py
```
You can SSH to the hal21 server and use the Vim editor to write code. Or, another way is to go to the [CoCalc page that has the code](https://cocalc.createlab.org:8443/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/files/air-src/automate-plume-viz/automate_plume_viz.py?session=default), and edit the code using the CoCalc interface. To run the code at the background (when you need to process a lot of dates), you can run the code using the CoCalc interface, and the script will run until end. Or, you can use the following command on the CoCalc terminal, which use the Linux screen command:
```sh
sh bg.sh python automate_plume_viz.py
```
After running the script, the videos and other related files will be stored in "automate-plume-viz/data/rgb/" and you need to run the following command to copy the videos to the place that can be accessed by the front-end website:
```sh
cp /projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/air-src/automate-plume-viz/data/rgb/*/*.mp4 /projects/cocalc-www.createlab.org/pardumps/video/
```

TODO: talk about how to copy files to the front-end website.

TODO: talk about how to copy EarthTime layers before running the hysplit simulation.
