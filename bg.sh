#!/bin/sh

# This script runs a python script using screen command
# For example:
# sh bg.sh python train.py i3d-rgb

# Get file path
if [ "$1" != "" ] && [ "$2" != "" ]
then
  echo "Run: $1 $2 $3 $4 $5 $6"
else
  echo "Usage examples:\n\
  sh bg.sh python automate_plume_viz.py"
  exit 1
fi

# Delete existing screen
for session in $(screen -ls | grep -o "[0-9]*.$1.$2.$3")
do
  screen -S "${session}" -X quit
  sleep 2
done

# Delete the log
rm screenlog.0

# For python in conda env in Ubuntu
screen -dmSL "$1.$2.$3" bash -c "export PATH='/projects/anaconda3/bin/:$PATH'; . '/projects/anaconda3/bin/activate'; $1 $2 $3 $4 $5 $6"

# List screens
screen -ls
exit 0
