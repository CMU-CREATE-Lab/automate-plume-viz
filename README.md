SSH to the hal21 server and clone this repository
```sh
ssh [USER_NAME]@hal21.andrew.cmu.edu
cd /projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/air-src/
git clone https://github.com/CMU-CREATE-Lab/automate-plume-viz.git
```
Change the permission of the folder so that the IPython Notebook can read that.
```sh
chmod 777 automate-plume-viz
```
When creating a file, make sure to cd to the directory and copy the empty template, so that git can add the file without permission problems. Note that we also need to change the permission to 777 so that the IPython Notebook can read the file.
```sh
cd /projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/air-src/automate-plume-viz
cp template/empty.ipynb my_file.ipynb
chmode 777 my_file.ipynb
```
