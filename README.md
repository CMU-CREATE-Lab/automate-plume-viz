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
Run the script using the screen command
```sh
sh bg.sh python automate_plume_viz.py
```
