"""
The main script for processing the plume visualization videos
(copy and modify this script for your own needs)
"""


import sys, os, traceback, urllib, json, time
import pandas as pd
from multiprocessing.dummy import Pool
from cached_hysplit_run_lib import DispersionSource
from automate_plume_viz import get_time_range_list, generate_metadata, check_and_create_dir, simulate_worker, is_url_valid, get_frames, get_all_dir_names_in_folder, unzip_and_rename, create_video


def genetate_earthtime_data(o_url):
    print("Generate EarthTime data...")

    # IMPORTANT: you need to specify the dates that we want to process
    date_list = []
    date_list.append(get_time_range_list(["2019-03-03", "2019-03-04"], duration=24, offset_hours=3))
    #date_list.append(get_start_end_time_list("2019-04-01", "2019-05-01", offset_hours=3))
    #date_list.append(get_start_end_time_list("2019-12-01", "2020-01-01", offset_hours=3))
    #date_list.append(get_start_end_time_list("2020-01-01", "2020-08-01", offset_hours=3))

    # Specify the starting and ending time
    df_layer, df_share_url, df_img_url, file_name, start_d, end_d = None, None, None, None, None, None
    for i in range(len(date_list)):
        sd, ed = date_list[i]
        if i == 0: # batch 1
            redo = 1
        elif i == 1: # batch 2
            redo = 2
        else: # other batch
            redo = 0
        # IMPORTANT: for your application, change the prefix, otherwise your data will be mixed with others
        # IMPORTANT: if you do not want to visualize smell reports, set add_smell to False
        dl, ds, di, fn = generate_metadata(sd, ed, url_partition=4, img_size=540, redo=redo, prefix="plume_",
                add_smell=True, lat="40.42532", lng="-79.91643", zoom="9.233", credits="CREATE Lab",
                category="Plume Viz", name_prefix="PARDUMP ", file_path=o_url)
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


def run_hysplit(o_root, start_d, file_name, o_url, num_workers=4):
    print("Run Hysplit model...")

    check_and_create_dir(o_root)

    # Location of the sources of pollution
    # IMPORTANT: for your application, change these pollution sources
    sources = [
        DispersionSource(name='Irvin',lat=40.328015, lon=-79.903551, minHeight=0, maxHeight=50),
        DispersionSource(name='ET',lat=40.392967, lon=-79.855709, minHeight=0, maxHeight=50),
        DispersionSource(name='Clairton',lat=40.305062, lon=-79.876692, minHeight=0, maxHeight=50),
        DispersionSource(name='Cheswick',lat=40.538261, lon=-79.790391, minHeight=0, maxHeight=50)]

    # Prepare the list of dates for running the simulation
    start_time_eastern_all = start_d.strftime("%Y-%m-%d %H:%M").values

    # Prepare the list of file names
    o_file_all = o_root + file_name.values + ".bin"

    # Prepare the list of URLs for checking if the file exists in the remote server
    # IMPORTANT: if you are doing experiments on the particle files,
    # ...make sure you set o_url_all to [None]*len(file_name.values)
    # ...otherwise the code will not run because the particle files aleady exist in the remote URLs
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


def generate_plume_viz_json(start_d, end_d):
    print("Generate the json file for the front-end...")

    # Get the number of smell reports in the desired dates
    time_list = list(map(lambda x: x.timestamp(), start_d.to_pydatetime()))
    time_list += list(map(lambda x: x.timestamp(), end_d.to_pydatetime()))
    start_time = int(min(time_list)) - 5000
    end_time = int(max(time_list)) + 5000
    smell_pgh_api_url = "https://api.smellpittsburgh.org/api/v2/smell_reports?group_by=day&aggregate=true&smell_value=3%2C4%2C5&start_time=" + str(start_time) + "&end_time=" + str(end_time) + "&state_ids=1&timezone_string=America%252FNew_York"
    try:
        print("\t{Request} %s\n" % smell_pgh_api_url)
        response = urllib.request.urlopen(smell_pgh_api_url)
        smell_counts = json.load(response)
        print("\t{Done} %s\n" % smell_pgh_api_url)
    except Exception:
        traceback.print_exc()
        smell_counts = None

    # Create the json object (for front-end)
    # TODO: instead of using start_d and end_d, check existing videos and generate the json file
    gp_end_d = end_d.groupby(end_d.year)
    viz_json = {}
    for k in gp_end_d:
        g_json = {"columnNames": ["label", "color", "file_name", "date"], "data": []}
        for d in sorted(gp_end_d[k].to_pydatetime()):
            label = d.strftime("%b %d")
            color = -1 if smell_counts is None else smell_counts[d.strftime("%Y-%m-%d")]
            vid_fn = d.strftime("%Y%m%d")
            vid_path = "data/rgb/" + vid_fn + "/" + vid_fn + ".mp4"
            if os.path.isfile(vid_path):
                # Only add to json if the file exists
                g_json["data"].append([label, color, vid_fn + ".mp4", d.strftime("%Y-%m-%d")])
        viz_json[k] = g_json

    # Save the json for the front-end visualization website
    p = "data/plume_viz.json"
    with open(p, "w") as f:
        json.dump(viz_json, f)
    os.chmod(p, 0o777)


def main(argv):
    """The main function"""
    if len(argv) < 2:
        print("Usage:")
        print("python automate_plume_viz.py genetate_earthtime_data")
        print("python automate_plume_viz.py run_hysplit")
        print("python automate_plume_viz.py download_video_frames")
        print("python automate_plume_viz.py rename_video_frames")
        print("python automate_plume_viz.py create_all_videos")
        print("python automate_plume_viz.py generate_plume_viz_json")
        print("python automate_plume_viz.py pipeline")
        return

    program_start_time = time.time()

    # Specify the path on the server that stores the bin files
    o_root = "/projects/earthtime/air-src/automate-plume-viz/data/bin/"

    # Specify the URL for accessing the bin files
    o_url = "https://cocalc-www.createlab.org/pardumps/plumeviz/bin/"

    # Run the following line first to generate EarthTime layers
    # IMPORTANT: you need to copy and paste the layers to the EarthTime layers CSV file
    start_d, end_d, file_name, df_share_url, df_img_url = genetate_earthtime_data(o_url)
    if argv[1] == "genetate_earthtime_data":
        print("END")
        return

    # Then run the following to create hysplit simulation files
    if argv[1] in ["run_hysplit", "pipeline"]:
        run_hysplit(o_root, start_d, file_name, o_url)

    # Next, run the following to download videos
    # IMPORTANT: if you forgot to copy and paste the EarthTime layers, this step will fail
    if argv[1] in ["download_video_frames", "pipeline"]:
        download_video_frames(o_url, df_share_url, df_img_url)

    # Then, rename files to epochtime
    if argv[1] in ["rename_video_frames", "pipeline"]:
        rename_video_frames()

    # Then, create all videos
    if argv[1] in ["create_all_videos", "pipeline"]:
        create_all_videos()

    # Finally, generate the json file for the front-end website
    # Copy and paste the json file to the front-end plume visualization website
    if argv[1] in ["generate_plume_viz_json", "pipeline"]:
        generate_plume_viz_json(start_d, end_d)

    program_run_time = (time.time()-program_start_time)/60
    print("Took %.2f minutes to run the program" % program_run_time)


if __name__ == "__main__":
    main(sys.argv)
