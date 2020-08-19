import sys
import os
import importlib
import re, array, csv, datetime, glob, json, math, random, stat
import pytz, datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import urllib.parse
import urllib.request
from multiprocessing.dummy import Pool
from itertools import product
import time
from os import listdir
from os.path import isfile, join, isdir
from zipfile import ZipFile
import shutil
import cv2 as cv
from PIL import Image, ImageFont, ImageDraw
from pardumpdump_util import *


o_root = "/projects/cocalc-www.createlab.org/pardumps/"


# This is a utility function for running other ipython notebooks
def exec_ipynb(filename_or_url):
    nb = (requests.get(filename_or_url).json() if re.match(r'https?:', filename_or_url) else json.load(open(filename_or_url)))
    if(nb['nbformat'] >= 4):
        src = [''.join(cell['source']) for cell in nb['cells'] if cell['cell_type'] == 'code']
    else:
        src = [''.join(cell['input']) for cell in nb['worksheets'][0]['cells'] if cell['cell_type'] == 'code']
    tmpname = '/tmp/%s-%s-%d.py' % (os.path.basename(filename_or_url),
                                    datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'),
                                    os.getpid())
    src = '\n\n\n'.join(src)
    open(tmpname, 'w').write(src)
    code = compile(src, tmpname, 'exec')
    exec(code, globals())


# Given starting and ending date string
# Get a list of starting and ending datetime objects
# Input:
#   start_date_eastern: the date to start in EST time, e.g., "2019-01-01"
#   end_date_eastern: the date to start in EST time, e.g., "2020-01-01"
#   offset_hour: time offset in hours, for example, if this is 3, then it starts from 12-3=9 p.m. instead of 12 a.m.
# Output:
#   start_d: a pandas DatetimeIndex object, indicating the list of starting times
#   end_d: a pandas DatetimeIndex object, indicating the list of ending times
def get_start_end_time_list(start_date_eastern, end_date_eastern, offset_hours=3):
    offset_d = pd.Timedelta(offset_hours, unit="h")
    start_d = pd.date_range(start=start_date_eastern, end=end_date_eastern, closed="left", tz="US/Eastern") - offset_d
    end_d = pd.date_range(start=start_date_eastern, end=end_date_eastern, closed="right", tz="US/Eastern") - offset_d
    return (start_d, end_d)


# Convert lists of starting and ending date strings to objects
# Input:
#   start_date_str_list: a list of date strings, e.g., ["2019-04-23", "2019-12-22", "2020-02-05"]
#   duration: the number of hours for each time range, e.g., 24
#   offset_hour: time offset in hours, for example, if this is 3, then it starts from 12-3=9 p.m. instead of 12 a.m.
# Output:
#   start_d: a pandas DatetimeIndex object, indicating the list of starting times
#   end_d: a pandas DatetimeIndex object, indicating the list of ending times
def get_time_range_list(start_date_str_list, duration=24, offset_hours=3):
    offset_d = pd.Timedelta(offset_hours, unit="h")
    start_d = pd.DatetimeIndex(data=start_date_str_list, tz="US/Eastern") - offset_d
    end_d = start_d + pd.Timedelta(duration, unit="h")
    return (start_d, end_d)


# Generate the EarthTime layers and the thumbnail server urls
# These urls can be called later to obtain video frames
# Input:
#   start_d: a pandas DatetimeIndex object, indicating the list of starting times
#   end_d: a pandas DatetimeIndex object, indicating the list of ending times
#   url_partition: the number of partitions for the thumbnail server request for getting images of video frames
# Output:
#   df_layer: the pandas dataframe for the EarthTime layer document
#   df_share_url: the pandas dataframe for the share urls
#   df_img_url: the pandas dataframe for the thumbnail server urls to get images of video frames
#   start_d: a pandas series of the starting datetime object in EST time
#   file_name: a list of file names
#   redo: this is a number to force the server to avoid using the cached file
def generate_metadata(start_d, end_d, url_partition=4, img_size=540, redo=0):
    if url_partition < 1:
        url_partition = 1
        print("Error! url_partition is less than 1. Set the url_partition to 1 to fix the error.")

    # Create rows in the EarthTime layer document
    df_template = pd.read_csv("data/earth_time_layer_template.csv")
    df_layer = pd.concat([df_template]*len(start_d), ignore_index=True)
    file_name = "plume_" + end_d.strftime("%Y%m%d")
    share_link_id = file_name
    start_d_utc = start_d.tz_convert("UTC")
    end_d_utc = end_d.tz_convert("UTC")
    df_layer["Start date"] = start_d_utc.strftime("%Y%m%d%H%M%S")
    df_layer["End date"] = end_d_utc.strftime("%Y%m%d%H%M%S")
    df_layer["Share link identifier"] = share_link_id
    df_layer["Name"] = "PARDUMP " + end_d.strftime("%Y-%m-%d")
    df_layer["URL"] = "https://cocalc-www.createlab.org/pardumps/" + file_name + ".bin"

    # Create rows of share URLs
    et_root_url = "https://headless.earthtime.org/#"
    et_part = "v=40.42532,-79.91643,9.233,latLng&ps=2400&startDwell=0&endDwell=0"
    ts_root_url = "https://thumbnails-earthtime.cmucreatelab.org/thumbnail?"
    ts_part = "&width=%d&height=%d&format=zip&fps=30&tileFormat=mp4&startDwell=0&endDwell=0&fromScreenshot&disableUI&redo=%d" % (img_size, img_size, redo)
    share_url_ls = [] # EarthTime share urls
    dt_share_url_ls = [] # the date of the share urls
    img_url_ls = [] # thumbnail server urls
    dt_img_url_ls = [] # the date of the thumbnail server urls

    #TODO: for testing
    share_link_id += "_v2"
    df_layer["Share link identifier"] = share_link_id
    df_layer["Name"] += " v2"
    df_layer["Vertex Shader"] = "WebGLVectorTile2.particleAltFadeVertexShader"
    df_layer["Fragment Shader"] = "WebGLVectorTile2.particleAltFadeFragmentShader"
    et_root_url = "https://headless-rsargent.earthtime.org/#"

    for i in range(len(start_d_utc)):
        sdt = start_d_utc[i]
        edt = end_d_utc[i]
        # Add the original url
        sdt_str = sdt.strftime("%Y%m%d%H%M%S")
        edt_str = edt.strftime("%Y%m%d%H%M%S")
        date_str = sdt_str[:8]
        bt = "bt=" + sdt_str + "&"
        et = "et=" + edt_str + "&"
        l = "l=bdrk_detailed,smell_my_city_pgh_reports_top," + share_link_id[i] + "&"
        share_url_ls.append(et_root_url + l + bt + et + et_part)
        dt_share_url_ls.append(date_str)
        # Add the thumbnail server url
        time_span = (edt - sdt) / url_partition
        for j in range(url_partition):
            std_j = sdt + time_span*j
            edt_j = std_j + time_span
            std_j_str = std_j.strftime("%Y%m%d%H%M%S")
            edt_j_str = edt_j.strftime("%Y%m%d%H%M%S")
            bt_j = "bt=" + std_j_str + "&"
            et_j = "et=" + edt_j_str + "&"
            rt = "root=" + urllib.parse.quote(et_root_url + l + bt_j + et_j + et_part, safe="") + "&"
            img_url_ls.append(ts_root_url + rt + ts_part)
            dt_img_url_ls.append(date_str)
    df_share_url = pd.DataFrame(data={"share_url": share_url_ls, "date": dt_share_url_ls})
    df_img_url = pd.DataFrame(data={"img_url": img_url_ls, "date": dt_img_url_ls})

    # return the data
    return (df_layer, df_share_url, df_img_url, file_name)


# Run the HYSPLIT simulation
# Input:
#   start_time_eastern: for different dates, use format "2020-03-30 00:00"
#   o_file: file path to save the simulation result, e.g., "/projects/cocalc-www.createlab.org/pardumps/test.bin"
#   sources: location of the sources of pollution, in an array of DispersionSource objects
#   emit_time_hrs: affects the emission time for running each Hysplit model
#   duration: total time (in hours) for the simulation, use 24 for a total day, use 12 for testing
#   filter_ratio: the ratio that the points will be dropped (e.g., 0.8 means dropping 80% of the points)
def simulate(start_time_eastern, o_file, sources, emit_time_hrs=1, duration=24, filter_ratio=0.8):
    print("="*100)
    print("="*100)
    print("start_time_eastern: %s" % start_time_eastern)
    print("o_file: %s" % o_file)

    # Run simulation and get the folder list (the generated files are cached)
    path_list = []
    for source in sources:
        path_list += getMultiHourDispersionRunsParallel(
                source,
                parse_eastern(start_time_eastern),
                emit_time_hrs,
                duration,
                HysplitModelSettings(initdModelType=InitdModelType.ParticleHV, hourlyPardump=False))
    print("len(path_list)=%d" % len(path_list))

    # Save pdump text files (the generated files are cached)
    pdump_txt_list = []
    for folder in path_list:
        if not findInFolder(folder,'PARDUMP*.txt'):
            pdump = findInFolder(folder,'PARDUMP.*')
            cmd = "/opt/hysplit/exec/par2asc -i%s -o%s" % (pdump, pdump+".txt")
            if pdump.find('.txt') == -1:
                pdump_txt_list.append(pdump+".txt")
            print("Run Hysplit for %s" % pdump)
            subprocess_check(cmd)
        else:
            pdump_txt = findInFolder(folder,'PARDUMP*.txt')
            pdump_txt_list.append(pdump_txt)
    print("len(pdump_txt_list)=%d" % len(pdump_txt_list))

    # Add color
    cmap = "viridis"
    c = plt.get_cmap(cmap)
    c.colors
    colors = np.array(c.colors)
    colors *= 255
    colormap = np.uint8(colors.round())
    colormap = colormap.reshape([1,256,3])
    cmaps = [
        [[250, 255, 99]],
        [[250, 255, 99],[99, 255, 206]],
        [[250, 255, 99],[99, 255, 206],[206, 92, 247]],
        [[250, 255, 99],[99, 255, 206],[206, 92, 247],[255, 119, 0]]
    ]
    print("Creating %s" % o_file)
    create_multisource_bin(pdump_txt_list, o_file, len(sources), False, cmaps, duration, filter_ratio=filter_ratio)
    print("Created %s" % o_file)
    os.chmod(o_file, 0o777)

    # Cleanup files
    print("Cleaning files...")
    for folder in path_list:
        pdump_txt = findInFolder(folder,'PARDUMP*.txt')
        print("Remove file %s" % pdump_txt)
        os.remove(pdump_txt)


# The parallel worker for simulation
def simulate_worker(start_time_eastern, o_file, sources):
    # Skip if the file exists
    if os.path.isfile(o_file):
        print("File already exists %s" % o_file)
        return True

    # HYSPLIT Simulation
    try:
        simulate(start_time_eastern, o_file, sources, emit_time_hrs=1, duration=24, filter_ratio=0.8)
        return True
    except Exception as ex:
        print("\t{%s} %s\n" % (ex, o_file))
        return False


# Call the thumbnail server to generate and get video frames
# Then save the video frames
# Input:
#   df_img_url: the pandas dataframe generated by using the generate_metadata function
#   dir_p: the folder path for saving the files
#   num_try: the number of times that the function has been called
#   num_workers: the number of workers to download the frames
def get_frames(df_img_url, dir_p="data/rgb/", num_try=0, num_workers=4):
    print("="*100)
    print("="*100)
    print("This function has been called for %d times." % num_try)
    if num_try > 30:
        print("Terminate the recursive call due to many errors. Please check manually.")
        return
    num_errors = 0
    arg_list = []
    # Construct the lists of urls and file paths
    for dt, df in df_img_url.groupby("date"):
        img_url_list = list(df["img_url"])
        dir_p_dt = dir_p + dt + "/"
        check_and_create_dir(dir_p) # need this line to set the permission
        check_and_create_dir(dir_p_dt)
        for i in range(len(img_url_list)):
            arg_list.append((img_url_list[i], dir_p_dt + str(i) + ".zip"))
    # Download the files in parallel
    result = Pool(num_workers).starmap(urlretrieve_worker, arg_list)
    for r in result:
        if r: num_errors += 1
    if num_errors > 0:
        print("="*60)
        print("Has %d errors. Need to do again." % num_errors)
        num_try += 1
        get_frames(df_img_url, num_try=num_try)
    else:
        print("DONE")


# The worker for getting the video frames
# Input:
#   url: the url for getting the frames
#   file_p: the path for saving the file
#   idx: the index of the worker
def urlretrieve_worker(url, file_p):
    time.sleep(1) # sleep to prevent calling the server too fast
    error = False
    if os.path.isfile(file_p): # skip if the file exists
        print("\t{File exists} %s\n" % file_p)
        return error
    try:
        print("\t{Request} %s\n" % url)
        urllib.request.urlretrieve(url, file_p)
        os.chmod(file_p, 0o777)
        print("\t{Done} %s\n" % url)
    except Exception as ex:
        print("\t{%s} %s\n" % (ex, url))
        error = True
    return error


# Check if a directory exists, if not, create it
def check_and_create_dir(path):
    if path is None: return
    dir_name = os.path.dirname(path)
    if dir_name != "" and not os.path.exists(dir_name):
        try: # this is used to prevent race conditions during parallel computing
            os.makedirs(dir_name)
            os.chmod(dir_name, 0o777)
        except Exception as ex:
            print(ex)


# Unzip the video frames and rename them to the correct datetime
# Input:
#   in_dir_p: path to the folder that has the zip file for one day's data
#   out_dir_p: path to the folder that will store the output frames
#   offset_hour: time offset in hours, for example, if this is 3, then it starts from 12-3=9 p.m. instead of 12 a.m.
def unzip_and_rename(in_dir_p, out_dir_p, offset_hours=3):
    # Compute the number of partitions
    num_partitions = 0
    for fn in get_all_file_names_in_folder(in_dir_p):
        if ".zip" not in fn: continue
        num_partitions += 1

    # Unzip each partition
    start_dt_str = re.findall(r"\d{8}", in_dir_p)[0]
    start_dt = datetime.datetime.strptime(start_dt_str, "%Y%m%d")
    start_dt = pytz.timezone("US/Eastern").localize(start_dt)
    start_dt = start_dt - pd.Timedelta(offset_hours, unit="h")
    time_span = pd.Timedelta(24 / num_partitions, unit="h")
    num_files_per_partition = 0
    for i in range(num_partitions):
        start_dt_partition = start_dt + time_span * i
        p_zip = in_dir_p + "%d.zip" % i
        p_unzip = in_dir_p + str(i) + "/"
        del_dir(p_unzip)
        print("Extract " + p_zip + " to " + p_unzip)
        with ZipFile(p_zip, "r") as zip_obj:
            zip_obj.extractall(p_unzip)
            os.chmod(p_unzip, 0o777)
            for dn in get_all_dir_names_in_folder(p_unzip):
                os.chmod(p_unzip + dn, 0o777)
            # Count the number of png files
            fn_list = get_all_file_names_in_folder(p_unzip + "frames/")
            if num_files_per_partition == 0:
                for fn in fn_list:
                    if "frame" in fn and ".png" in fn:
                        num_files_per_partition += 1
            # Loop and rename the files
            time_span_frame = pd.Timedelta(time_span/(num_files_per_partition - 1), unit="h")
            for fn in fn_list:
                frame_number = int(re.findall(r"\d{6}", fn)[0]) - 1
                frame_epochtime = start_dt_partition + time_span_frame * frame_number
                frame_epochtime = round(frame_epochtime.timestamp())
                new_fn = str(frame_epochtime) + ".png"
                os.rename(p_unzip + "frames/" + fn, p_unzip + "frames/" + new_fn)

    # Put files in one folder
    del_dir(out_dir_p)
    check_and_create_dir(out_dir_p)
    for i in range(num_partitions):
        p = in_dir_p + str(i) + "/frames/"
        for fn in get_all_file_names_in_folder(p):
            os.rename(p + fn, out_dir_p + fn)
        del_dir(in_dir_p + str(i))

    # Set permissions
    for fn in get_all_file_names_in_folder(out_dir_p):
        os.chmod(out_dir_p + fn, 0o777)
    print("DONE")


# Delete a directory and all its contents
def del_dir(dir_p):
    if not os.path.isdir(dir_p): return
    try:
        shutil.rmtree(dir_p)
    except Exception as ex:
        print(ex)


# Return a list of all files in a folder
def get_all_file_names_in_folder(path):
    return [f for f in listdir(path) if isfile(join(path, f))]


# Return a list of all directories in a folder
def get_all_dir_names_in_folder(path):
    return [f for f in listdir(path) if isdir(join(path, f))]


# Add caption to the images by its file name (epochtime)
# Then merge these images into a video
# Input:
#   in_dir_p: path to the folder that contains video frames
#   out_file_p: the path to the file that will store the video
#   font_p: the path to the font file for the caption
#   reduce_size: if True, will reduce file size using ffmpeg
def create_video(in_dir_p, out_file_p, font_p, fps=30, reduce_size=False):
    print("Process images in %r" % in_dir_p)
    out_file_p_tmp = out_file_p + ".mp4"
    time_list = []
    for fn in get_all_file_names_in_folder(in_dir_p):
        time_list.append(int(fn.split(".")[0]))
    time_list = sorted(time_list)
    width, height = Image.open(in_dir_p + "%d.png" % time_list[0]).size
    fourcc = cv.VideoWriter_fourcc(*"avc1")
    video = cv.VideoWriter(out_file_p_tmp, fourcc, fps, (width, height))
    for t in time_list:
        img = Image.open(in_dir_p + "%d.png" % t)
        draw = ImageDraw.Draw(img)
        font = ImageFont.truetype(font_p, 40)
        dt = datetime.datetime.fromtimestamp(t).astimezone(pytz.timezone("US/Eastern"))
        caption = dt.strftime("%Y-%m-%d,  %H:%M")
        draw.text((10, 3), caption, (0,255,255), font=font)
        video.write(cv.cvtColor(np.array(img), cv.COLOR_RGB2BGR))
    cv.destroyAllWindows()
    video.release()
    if reduce_size:
        print("Reducing file size...")
        subprocess.call("ffmpeg -i %s -vf scale=540:540 -b:v 1200k -bufsize 1200k -y %s" % (out_file_p_tmp, out_file_p), shell=True)
        # TODO: the above ffmpeg compress too much, need to try the following one
        #subprocess.call("ffmpeg -i %s -vf scale=540:540 -vcodec libx264 -preset slow -pix_fmt yuv420p -crf 20 -movflags faststart -y %s" % (out_file_p_tmp, out_file_p), shell=True)
        os.remove(out_file_p_tmp)
    else:
        os.rename(out_file_p_tmp, out_file_p)
    os.chmod(out_file_p, 0o777)
    print("DONE saving video to %r" % out_file_p)


def load_utility():
    print("Load utility functions...")

    # Load utility functions from another ipython notebook
    root_dir = "/projects/9ab71616-fcde-4524-bf8f-7953c669ebbb/air-src/"
    os.chdir(root_dir + "linRegModel")
    exec_ipynb("./cachedHysplitRunLib.ipynb")
    exec_ipynb("../../src/python-utils/utils.ipynb")
    os.chdir(root_dir + "automate-plume-viz/")


def genetate_earthtime_data():
    print("Generate EarthTime data...")

    # Specify the dates that we want to process
    date_list = []

    # Date batch
    #start_d_str_list = ["2020-02-03", "2020-02-06", "2020-02-17", "2020-02-23", "2020-02-24"]
    #date_list.append(get_time_range_list(start_d_str_list, duration=24, offset_hours=3))

    # Date batch 1
    date_list.append(get_start_end_time_list("2019-04-01", "2019-05-01", offset_hours=3))

    # Date batch 2
    date_list.append(get_start_end_time_list("2019-12-01", "2020-01-01", offset_hours=3))

    # Date batch 3
    date_list.append(get_start_end_time_list("2020-01-01", "2020-08-01", offset_hours=3))

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
        dl, ds, di, fn = generate_metadata(sd, ed, url_partition=4, redo=redo)
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


def run_hysplit(start_d, file_name, num_workers=4):
    print("Run Hysplit model...")

    # Location of the sources of pollution
    sources = [
        DispersionSource(name='Irvin',lat=40.328015, lon=-79.903551, minHeight=0, maxHeight=50),
        DispersionSource(name='ET',lat=40.392967, lon=-79.855709, minHeight=0, maxHeight=50),
        DispersionSource(name='Clairton',lat=40.305062, lon=-79.876692, minHeight=0, maxHeight=50),
        DispersionSource(name='Cheswick',lat=40.538261, lon=-79.790391, minHeight=0, maxHeight=50)]

    # Prepare the list of dates for running the simulation
    start_time_eastern_all = start_d.strftime("%Y-%m-%d %H:%M").values
    o_file_all = o_root + file_name.values + ".bin"

    # Run the simulation for each date in parallel (be aware of the memory usage)
    arg_list = []
    for i in range(len(o_file_all)):
        arg_list.append((start_time_eastern_all[i], o_file_all[i], sources))
    result = Pool(num_workers).starmap(simulate_worker, arg_list)
    #simulate_worker(start_time_eastern_all[0], o_file_all[0], sources)


def download_video_frames(df_share_url, df_img_url):
    print("Download video frames from the thumbnail server...")

    # Make sure that the dates have the hysplit simulation results
    date_has_hysplit = []
    for idx, row in df_share_url.iterrows():
        if os.path.isfile(o_root + "plume_" + row["date"] + ".bin"):
            date_has_hysplit.append(row["date"])
    get_frames(df_img_url[df_img_url["date"].isin(date_has_hysplit)], dir_p="data/rgb/")


def rename_video_frames():
    print("Rename all video frames using epochtime...")

    # For each date, unzip and rename the video frames
    for dn in get_all_dir_names_in_folder("data/rgb/"):
        in_dir_p = "data/rgb/" + dn + "/"
        unzip_and_rename(in_dir_p, in_dir_p+"frames/", offset_hours=3)


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
    except Exception as ex:
        print("\t{%s} %s\n" % (ex, smell_pgh_api_url))
        smell_counts = None

    # Create the json object (for front-end)
    viz_json = {"columnNames": ["label", "color", "file_name"], "data": []}
    for d in sorted(end_d.to_pydatetime()):
        label = d.strftime("%b %d")
        if smell_counts is None:
            color = -1
        else:
            color = smell_counts[d.strftime("%Y-%m-%d")]
        vid_fn = d.strftime("%Y%m%d")
        vid_path = "data/rgb/" + vid_fn + "/" + vid_fn + ".mp4"
        if os.path.isfile(vid_path):
            # Only add to json if the file exists
            viz_json["data"].append([label, color, vid_fn + ".mp4"])

    # Save the json for the front-end visualization website
    p = "data/plume_viz.json"
    with open(p, "w") as f:
        json.dump(viz_json, f)
    os.chmod(p, 0o777)


# The main function
def main(argv):
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

    load_utility()

    # Run the following line first to generate earthtime layers
    # Copy and paste the layers to the earthtime layers CSV file
    start_d, end_d, file_name, df_share_url, df_img_url = genetate_earthtime_data()
    if argv[1] == "genetate_earthtime_data": return

    # Then run the following to create hysplit simulation files
    if argv[1] in ["run_hysplit", "pipeline"]:
        run_hysplit(start_d, file_name)

    # Next, run the following to download videos
    if argv[1] in ["download_video_frames", "pipeline"]:
        download_video_frames(df_share_url, df_img_url)

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
