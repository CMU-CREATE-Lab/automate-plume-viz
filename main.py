"""
The main script for processing the plume visualization videos
(copy and modify this script for your own needs)
"""


import sys, os, time
import pandas as pd
from datetime import date
from datetime import timedelta
from multiprocessing.dummy import Pool
from cached_hysplit_run_lib import DispersionSource
from automate_plume_viz import get_time_range_list, generate_metadata, simulate_worker, is_url_valid, get_frames, get_all_dir_names_in_folder, unzip_and_rename, create_video, generate_plume_viz_json, get_start_end_time_list


def genetate_earthtime_data(date_list, bin_url, url_partition, img_size, redo, prefix,
        add_smell, lat, lng, zoom, credits,  category, name_prefix, video_start_delay_hrs=0):
    print("Generate EarthTime data...")

    # Specify the starting and ending time
    df_layer, df_share_url, df_img_url, file_name, start_d, end_d = None, None, None, None, None, None
    sd, ed = date_list[0], date_list[1]
    dl, ds, di, fn = generate_metadata(sd, ed, video_start_delay_hrs=video_start_delay_hrs, 
            url_partition=url_partition, img_size=img_size, redo=redo, prefix=prefix,
            add_smell=add_smell, lat=lat, lng=lng, zoom=zoom, credits=credits,
            category=category, name_prefix=name_prefix, file_path=bin_url)
    if df_layer is None:
        df_layer, df_share_url, df_img_url, file_name, start_d, end_d = dl, ds, di, fn, sd, ed
    else:
        df_layer = pd.concat([df_layer, dl], ignore_index=True)
        df_share_url = pd.concat([df_share_url, ds], ignore_index=True)
        df_img_url = pd.concat([df_img_url, di], ignore_index=True)
        file_name = file_name.union(fn)
        start_d = start_d.union(sd)
        end_d = end_d.union(ed)

    # Save rows of EarthTime CSV layers to a file
    p = "data/earth_time_layer.csv"
    df_layer.to_csv(p, index=False)
    os.chmod(p, 0o777)

    # Save rows of share urls to a file
    p = "data/earth_time_share_urls.csv"
    df_share_url.to_csv(p, index=False)
    os.chmod(p, 0o777)

    # Save rows of thumbnail server urls to a file
    p = "data/earth_time_thumbnail_urls.csv"
    df_img_url.to_csv(p, index=False)
    os.chmod(p, 0o777)

    return (start_d, end_d, file_name, df_share_url, df_img_url)


def run_hysplit(sources, bin_root, start_d, end_d, file_name, bin_url=None, num_workers=4):
    print("Run Hysplit model...")

    # Prepare the list of dates for running the simulation
    start_time_eastern_all = start_d.strftime("%Y-%m-%d %H:%M").values

    # Prepare the list of file names
    bin_file_all = bin_root + file_name.values + ".bin"

    # Prepare the list of URLs for checking if the file exists in the remote server
    if bin_url is None:
        bin_url_all = [None]*len(file_name.values)
    else:
        bin_url_all = bin_url + file_name.values + ".bin"

    # Set default parameters (see the simulate function in automate_plume_viz.py to get more details)
    emit_time_hrs = 1
    duration = (end_d[0] - start_d[0]).days * 24  + (end_d[0] - start_d[0]).seconds / 3600
    filter_ratio = 0.8

    # Run the simulation for each date in parallel (be aware of the memory usage)
    arg_list = []
    print("Running hysplit simulation with duration: %s hours" % duration)
    for i in range(len(bin_file_all)):
        arg_list.append((start_time_eastern_all[i], bin_file_all[i], sources,
            emit_time_hrs, duration, filter_ratio, bin_url_all[i]))
    pool = Pool(num_workers)
    pool.starmap(simulate_worker, arg_list)
    pool.close()
    pool.join()


def download_video_frames(bin_url, df_share_url, df_img_url, prefix="plume_"):
    print("Download video frames from the thumbnail server...")

    # Make sure that the dates have the hysplit simulation results
    date_has_hysplit = []
    for idx, row in df_share_url.iterrows():
        fname = prefix + row["date"] + ".bin"
        print(fname)
        if is_url_valid(bin_url + fname):
            date_has_hysplit.append(row["date"])

    get_frames(df_img_url[df_img_url["date"].isin(date_has_hysplit)], dir_p="data/rgb/")


def create_all_videos(video_root):
    print("Create all videos...")

    font_p = "data/font/OpenSans-Regular.ttf"
    for dn in get_all_dir_names_in_folder("data/rgb/"):
        in_dir_p = "data/rgb/" + dn + "/"
        # Unzip and rename video frames
        frame_dir_p = in_dir_p + "frames/"
        if not os.path.isdir(frame_dir_p): # skip if video frames were unzipped
            print("Process %s..." % dn)
            status = unzip_and_rename(in_dir_p, frame_dir_p, offset_hours=3)
            if (status == 1):
                continue
        # Create video
        video_file_p = video_root + dn + ".mp4"
        if not os.path.isfile(video_file_p): # skip if the video exists
            create_video(frame_dir_p, video_file_p, font_p)
        else:
            print("Skip creating video since it exists...")


def main(argv):
    if len(argv) < 2:
        print("Usage:")
        print("python main.py genetate_earthtime_data")
        print("python main.py run_hysplit")
        print("python main.py download_video_frames")
        print("python main.py create_all_videos")
        print("python main.py generate_plume_viz_json")
        return

    program_start_time = time.time()

    # IMPORTANT: specify the path on the server that stores your particle bin files
    # bin_root = "[YOUR_PATH]/bin/"
    bin_root = None

    # IMPORTANT: specify the path on the server that stores your video files
    # bin_root = "[YOUR_PATH]/video/"
    video_root = None

    # IMPORTANT: specify the URL for accessing the bin files
    # bin_url = "https://[YOUR_URL_ROOT]/bin/"
    bin_url = None

    # IMPORTANT: specify the URL for accessing the video files
    # video_url = "https://[YOUR_URL_ROOT]/video/"
    video_url = None

    # IMPORTANT: specify a list to indicate the starting and ending dates to proces
    # You can use two supporing functions to generate the list, see below for examples
    # date_list = get_time_range_list(["2019-03-05", "2019-03-06"], duration=24, offset_hours=3)
    # date_list = get_start_end_time_list("2019-04-01", "2019-05-01", offset_hours=3)
    date_list = None

    # IMPORTANT: specify an unique string to prevent your data from mixing with others
    # IMPORTANT: do not set prefix to "plume_" which is used by the CREATE Lab's project
    prefix = None

    # IMPORTANT: specify a list of pollution sources
    # See below for an example using the DispersionSource class
    # sources = [DispersionSource(name='Irvin',lat=40.328015, lon=-79.903551, minHeight=0, maxHeight=50)]
    sources = []

    # IMPORTANT: specify the location of the map that you want to show, using lat, lng, and zoom
    # ...(lat means latitude, lng means longitude, zoom means the zoom level of the Google Map)
    lat, lng, zoom = None, None, None

    # IMPORTANT: if you do not want to show smell reports, set add_smell to False
    add_smell = True

    # IMPORTANT: if you want the thumbnail server to re-render video frames (e.g., for experiments), increase redo
    redo = 0

    # If you want the video to start after the model starts, specify the delay in hours
    video_start_delay_hrs = 2

    # Set the number of partitions of the URL for the thumbnail server to process in parallel
    url_partition = 4

    # Set the prefix of the names of the EarthTime layers
    # ...(will only affect the layers shown on the EarthTime system)
    name_prefix = "PARDUMP "

    # Set the credit of the EarthTime layer
    # ...(will only affect the layers shown on the EarthTime system)
    credits = "CREATE Lab"

    # Set the category of the EarthTime layer
    # ...(will only affect the layers shown on the EarthTime system)
    category = "Plume Viz"

    # Set the size of the output video (for both width and height)
    img_size = 540

    # IMPORTANT: below is the setting for the main project, you should not use these parameters
    # TODO: add a config file for the parameters
    bin_root = "/projects/aircocalc-www.createlab.org/pardumps/plumeviz/experiments/bin/stilt-mode/" # Yen-Chia's example (DO NOT USE)
    video_root = "/projects/aircocalc-www.createlab.org/pardumps/plumeviz/experiments/video/stilt-mode/" # Yen-Chia's example (DO NOT USE)
    bin_url = "https://aircocalc-www.createlab.org/pardumps/plumeviz/experiments/bin/stilt-mode/" # Yen-Chia's example (DO NOT USE)
    video_url = "https://aircocalc-www.createlab.org/pardumps/plumeviz/experiments/video/stilt-mode/" # Yen-Chia's example (DO NOT USE)
    prefix, lat, lng, zoom = "plume_stilt_", "40.42532", "-79.91643", "9.233"
    sources = [
        {
            "dispersion_source":DispersionSource(name='Irvin',lat=40.328015, lon=-79.903551, minHeight=0, maxHeight=50),
            "color": [250, 255, 99],
            "filter_out": .76
        },
        {
            "dispersion_source":DispersionSource(name='ET',lat=40.392967, lon=-79.855709, minHeight=0, maxHeight=50),
            "color": [99, 255, 206],
            "filter_out": .86
        },
        {
            "dispersion_source":DispersionSource(name='Clairton',lat=40.305062, lon=-79.876692, minHeight=0, maxHeight=50),
            "color": [206, 92, 247],
            "filter_out": .50
        },
        {
            "dispersion_source":DispersionSource(name='Cheswick',lat=40.538261, lon=-79.790391, minHeight=0, maxHeight=50),
            "color": [255, 119, 0],
            "filter_out": .89
        }
        ]
    date_list, redo = get_time_range_list(["2021-04-05"], duration=26, offset_hours=5), 6
    #date_list, redo = get_start:_end_time_list("2019-03-01", "2019-03-15", offset_hours=3), 1
    #date_list, redo = get_start_end_time_list("2021-04-08", "2021-04-09", offset_hours=3), 5
    #date_list, redo = get_start_end_time_list("2019-12-01", "2020-01-01", offset_hours=3), 2
    #date_list, redo = get_start_end_time_list("2021-04-05", "2021-04-06", offset_hours=3), 3
    #today = date.today().strftime("%Y-%m-%d")


    # Sanity checks
    assert(bin_root is not None), "you need to edit the path for storing hysplit particle files"
    assert(video_root is not None), "you need to edit the path for storing video files"
    assert(bin_url is not None), "you need to edit the URL for accessing the particle files"
    assert(video_url is not None), "you need to edit the URL for accessing the video files"
    assert(date_list is not None),"you need to specify the dates to process"
    assert(prefix is not None),"you need to specify the prefix of the unique share url"
    assert(len(sources) > 0),"you need to specify the pollution sources"
    assert(lat is not None),"you need to specify the latitude of the map"
    assert(lng is not None),"you need to specify the longitude of the map"
    assert(zoom is not None),"you need to specify the zoom level of the map"

    # Run the following line first to generate EarthTime layers
    # IMPORTANT: you need to copy and paste the generated layers to the EarthTime layers CSV file
    # ...check the README file about how to do this
    if argv[1] in ["genetate_earthtime_data", "run_hysplit", "download_video_frames"]:
        start_d, end_d, file_name, df_share_url, df_img_url = genetate_earthtime_data(date_list, 
                bin_url, url_partition, img_size, redo, prefix, add_smell, lat, lng, zoom,
                credits, category, name_prefix, video_start_delay_hrs)

    # Then run the following to create hysplit simulation files
    # IMPORTANT: after creating the bin files, you need to move them to the correct folder for public access
    # ... check the README file about how to copy and move the bin files
    # IMPORTANT: if you are doing experiments on creating the particle files,
    # ...make sure you set the input argument "bin_url" of the run_hysplit function to None
    # ...otherwise the code will not run because the particle files aleady exist in the remote URLs
    if argv[1] == "run_hysplit":
        run_hysplit(sources, bin_root, start_d, end_d, file_name, bin_url=bin_url)

    # Next, run the following to download videos
    # IMPORTANT: if you forgot to copy and paste the EarthTime layers, this step will fail
    # IMPORTANT: if you forgot to copy the bin files to the correct folder, this step will not do anything
    if argv[1] == "download_video_frames":
        download_video_frames(bin_url, df_share_url, df_img_url, prefix)

    # Then, create all videos
    # IMPORTANT: after creating the video files, you need to move them to the correct folder for public access
    # ... check the README file about how to copy and move the video files
    if argv[1] == "create_all_videos":
        create_all_videos(video_root)

    # Finally, generate the json file for the front-end website
    # IMPORTANT: you need to copy and paste the json file to the front-end plume visualization website
    # ...if you forgot to copy the video files to the correct folder, videos will not be found online
    if argv[1] == "generate_plume_viz_json":
        generate_plume_viz_json(video_url)

    program_run_time = (time.time()-program_start_time)/60
    print("Took %.2f minutes to run the program" % program_run_time)


if __name__ == "__main__":
    main(sys.argv)
