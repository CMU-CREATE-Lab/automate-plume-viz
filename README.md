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
Run the script using the screen command
```sh
sh bg.sh python automate_plume_viz.py
```
