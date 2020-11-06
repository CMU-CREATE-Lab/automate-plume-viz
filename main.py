"""
The main script for processing the plume visualization videos
(copy and modify this script for your own needs)
"""


import sys, os, traceback, time
import pandas as pd
from multiprocessing.dummy import Pool
from cached_hysplit_run_lib import DispersionSource
from automate_plume_viz import get_time_range_list, generate_metadata, check_and_create_dir, simulate_worker, is_url_valid, get_frames, get_all_dir_names_in_folder, unzip_and_rename, create_video, generate_plume_viz_json, get_start_end_time_list


def genetate_earthtime_data(date_list, o_url, url_partition, img_size, redo, prefix,
        add_smell, lat, lng, zoom, credits,  category, name_prefix):
    print("Generate EarthTime data...")

    # Specify the starting and ending time
    df_layer, df_share_url, df_img_url, file_name, start_d, end_d = None, None, None, None, None, None
    sd, ed = date_list[0], date_list[1]
    dl, ds, di, fn = generate_metadata(sd, ed, url_partition=url_partition, img_size=img_size,
            redo=redo, prefix=prefix, add_smell=add_smell, lat=lat, lng=lng, zoom=zoom, credits=credits,
            category=category, name_prefix=name_prefix, file_path=o_url)
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


def run_hysplit(sources, o_root, start_d, file_name, o_url=None, num_workers=4):
    print("Run Hysplit model...")

    check_and_create_dir(o_root)

    # Prepare the list of dates for running the simulation
    start_time_eastern_all = start_d.strftime("%Y-%m-%d %H:%M").values

    # Prepare the list of file names
    o_file_all = o_root + file_name.values + ".bin"

    # Prepare the list of URLs for checking if the file exists in the remote server
    if o_url is None:
        o_url_all = [None]*len(file_name.values)
    else:
        o_url_all = o_url + file_name.values + ".bin"

    # Set parameters (see the simulate function in automate_plume_viz.py to get more details)
    emit_time_hrs = 1
    duration = 24
    filter_ratio = 0.8

    # Run the simulation for each date in parallel (be aware of the memory usage)
    arg_list = []
    for i in range(len(o_file_all)):
        arg_list.append((start_time_eastern_all[i], o_file_all[i], sources,
            emit_time_hrs, duration, filter_ratio, o_url_all[i]))
    pool = Pool(num_workers)
    pool.starmap(simulate_worker, arg_list)
    pool.close()
    pool.join()


def download_video_frames(o_url, df_share_url, df_img_url):
    print("Download video frames from the thumbnail server...")
    # Make sure that the dates have the hysplit simulation results
    date_has_hysplit = []
    for idx, row in df_share_url.iterrows():
        fname = "plume_" + row["date"] + ".bin"
        if is_url_valid(o_url + fname):
            date_has_hysplit.append(row["date"])
    get_frames(df_img_url[df_img_url["date"].isin(date_has_hysplit)], dir_p="data/rgb/")


def rename_video_frames():
    print("Rename all video frames using epochtime...")

    # For each date, unzip and rename the video frames
    for dn in get_all_dir_names_in_folder("data/rgb/"):
        in_dir_p = "data/rgb/" + dn + "/"
        out_dir_p = in_dir_p + "frames/"
        if os.path.isdir(out_dir_p): continue
        try:
            unzip_and_rename(in_dir_p, out_dir_p, offset_hours=3)
        except Exception:
            traceback.print_exc()


def create_all_videos():
    print("Create all videos...")

    # Generate videos
    font_p = "data/font/OpenSans-Regular.ttf"
    for dn in get_all_dir_names_in_folder("data/rgb/"):
        in_dir_p = "data/rgb/" + dn + "/"
        video_path = in_dir_p + dn + ".mp4"
        if os.path.isfile(video_path): continue
        create_video(in_dir_p + "frames/", in_dir_p + dn + ".mp4", font_p)


def main(argv):
    """The main function"""
    if len(argv) < 2:
        print("Usage:")
        print("python main.py genetate_earthtime_data")
        print("python main.py run_hysplit")
        print("python main.py download_video_frames")
        print("python main.py rename_video_frames")
        print("python main.py create_all_videos")
        print("python main.py generate_plume_viz_json")
        return

    program_start_time = time.time()

    # IMPORTANT: specify the path on the server that stores your particle bin files
    # o_root = "[YOUR_PATH]/automate-plume-viz/data/bin/"
    o_root = None

    # IMPORTANT: specify the URL for accessing the bin files
    # o_url = "https://[YOUR_URL_ROOT]/bin/"
    o_url = None

    # IMPORTANT: specify the URL for accessing the video files
    # video_url = "https://[YOUR_URL_ROOT]/video/"
    video_url = None

    # IMPORTANT: specify a list to indicate the starting and ending dates to proces
    # You can use two supporing functions to generate the list, see below for examples
    # date_list = get_time_range_list(["2019-03-05", "2019-03-06"], duration=24, offset_hours=3)
    # date_list = get_start_end_time_list("2019-04-01", "2019-05-01", offset_hours=3)
    date_list = None

    # IMPORTANT: specify an unique string to prevent your data from mixing with others
    # Check the generate_metadata function docstring in automate_plume_viz.py for details
    prefix = None

    # IMPORTANT: specify a list of pollution sources
    # See below for an example using the DispersionSource class
    # sources = [DispersionSource(name='Irvin',lat=40.328015, lon=-79.903551, minHeight=0, maxHeight=50)]
    sources = []

    # IMPORTANT: below are the parameters that you can use by default
    # Check the generate_metadata function docstring in automate_plume_viz.py for details
    add_smell = True
    lat = "40.42532"
    lng = "-79.91643"
    zoom = "9.233"
    credits = "CREATE Lab"
    category = "Plume Viz"
    name_prefix = "PARDUMP "
    redo = 0
    url_partition = 4
    img_size = 540

    # Below is Yen-Chia Hsu's setting, you should not use these parameters
    #o_root = "/projects/earthtime/air-src/automate-plume-viz/data/bin/" # Yen-Chia's example (DO NOT USE)
    #o_url = "https://aircocalc-www.createlab.org/pardumps/plumeviz/bin/" # Yen-Chia's example (DO NOT USE)
    #video_url = "https://aircocalc-www.createlab.org/pardumps/plumeviz/video/" # Yen-Chia's example (DO NOT USE)
    #date_list, redo, prefix = get_time_range_list(["2019-03-09", "2019-03-10"], duration=24, offset_hours=3), 1, "plume_"
    #date_list, redo, prefix = get_start_end_time_list("2019-03-01", "2019-03-12", offset_hours=3), 1, "plume_"
    #date_list, redo, prefix = get_start_end_time_list("2019-04-01", "2019-05-01", offset_hours=3), 1, "plume_"
    #date_list, redo, prefix = get_start_end_time_list("2019-12-01", "2020-01-01", offset_hours=3), 2, "plume_"
    #date_list, redo, prefix = get_start_end_time_list("2020-01-01", "2020-08-01", offset_hours=3), 0, "plume_"
    #sources = [DispersionSource(name='Irvin',lat=40.328015, lon=-79.903551, minHeight=0, maxHeight=50), DispersionSource(name='ET',lat=40.392967, lon=-79.855709, minHeight=0, maxHeight=50), DispersionSource(name='Clairton',lat=40.305062, lon=-79.876692, minHeight=0, maxHeight=50), DispersionSource(name='Cheswick',lat=40.538261, lon=-79.790391, minHeight=0, maxHeight=50)]

    # Sanity checks
    assert(o_root is not None), "you need to edit the path for storing hysplit particle files"
    assert(o_url is not None), "you need to edit the URL for accessing the particle files"
    assert(video_url is not None), "you need to edit the URL for accessing the video files"
    assert(date_list is not None),"you need to specify the dates to process"
    assert(prefix is not None),"you need to specify the prefix of the unique share url"
    assert(len(sources) > 0),"you need to specify the pollution sources"

    # Run the following line first to generate EarthTime layers
    # IMPORTANT: you need to copy and paste the generated layers to the EarthTime layers CSV file
    # ...check the README file about how to do this
    start_d, end_d, file_name, df_share_url, df_img_url = genetate_earthtime_data(date_list, o_url,
            url_partition, img_size, redo, prefix, add_smell, lat, lng, zoom, credits, category, name_prefix)
    if argv[1] == "genetate_earthtime_data":
        print("END")
        return

    # Then run the following to create hysplit simulation files
    # IMPORTANT: after creating the bin files, you need to move them to the correct folder for public access
    # ... check the README file about how to copy and move the bin files
    # IMPORTANT: if you are doing experiments on creating the particle files,
    # ...make sure you set the input argument "o_url" of the run_hysplit function to None
    # ...otherwise the code will not run because the particle files aleady exist in the remote URLs
    if argv[1] == "run_hysplit":
        run_hysplit(sources, o_root, start_d, file_name, o_url=o_url)

    # Next, run the following to download videos
    # IMPORTANT: if you forgot to copy and paste the EarthTime layers, this step will fail
    # IMPORTANT: if you forgot to copy the bin files to the correct folder, this step will not do anything
    if argv[1] == "download_video_frames":
        download_video_frames(o_url, df_share_url, df_img_url)

    # Then, rename files to epochtime
    if argv[1] == "rename_video_frames":
        rename_video_frames()

    # Then, create all videos
    # IMPORTANT: after creating the video files, you need to move them to the correct folder for public access
    # ... check the README file about how to copy and move the video files
    if argv[1] == "create_all_videos":
        create_all_videos()

    # Finally, generate the json file for the front-end website
    # IMPORTANT: you need to copy and paste the json file to the front-end plume visualization website
    # ...if you forgot to copy the video files to the correct folder, videos will not be found online
    if argv[1] == "generate_plume_viz_json":
        generate_plume_viz_json(video_url)

    program_run_time = (time.time()-program_start_time)/60
    print("Took %.2f minutes to run the program" % program_run_time)


if __name__ == "__main__":
    main(sys.argv)
