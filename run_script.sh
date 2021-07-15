#!/bin/bash
. /projects/anaconda3/bin/activate
python main.py genetate_earthtime_data
python main.py run_hysplit
python main.py download_video_frames
python main.py create_all_videos
python main.py generate_plume_viz_json

echo "!!!DONE!!!"

