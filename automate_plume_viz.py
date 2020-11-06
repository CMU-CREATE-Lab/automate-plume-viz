"""
Automate the plume visualization using hysplit model simulation
"""


import os, re, datetime, json, pytz, subprocess, time, shutil, requests, traceback
import numpy as np
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import urllib.parse
import urllib.request
from multiprocessing.dummy import Pool
from os import listdir
from os.path import isfile, join, isdir
from zipfile import ZipFile
import cv2 as cv
from PIL import Image, ImageFont, ImageDraw
from utils import subprocess_check
from pardumpdump_util import findInFolder, create_multisource_bin
from cached_hysplit_run_lib import getMultiHourDispersionRunsParallel, parse_eastern, HysplitModelSettings, InitdModelType


def exec_ipynb(filename_or_url):
    """Load other ipython notebooks and import their functions"""
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


def get_start_end_time_list(start_date_eastern, end_date_eastern, offset_hours=3):
    """
    Given starting and ending date string, get a list of starting and ending datetime objects

    Input:
        start_date_eastern: the date to start in EST time, e.g., "2019-01-01"
        end_date_eastern: the date to start in EST time, e.g., "2020-01-01"
        offset_hour: time offset in hours, for example, if this is 3, then it starts from 12-3=9 p.m. instead of 12 a.m.

    Output:
        start_d: a pandas DatetimeIndex object, indicating the list of starting times
        end_d: a pandas DatetimeIndex object, indicating the list of ending times
    """
    offset_d = pd.Timedelta(offset_hours, unit="h")
    start_d = pd.date_range(start=start_date_eastern, end=end_date_eastern, closed="left", tz="US/Eastern") - offset_d
    end_d = pd.date_range(start=start_date_eastern, end=end_date_eastern, closed="right", tz="US/Eastern") - offset_d
    return (start_d, end_d)


def get_time_range_list(start_date_str_list, duration=24, offset_hours=3):
    """
    Convert lists of starting and ending date strings to objects

    Input:
        start_date_str_list: a list of date strings, e.g., ["2019-04-23", "2019-12-22", "2020-02-05"]
        duration: the number of hours for each time range, e.g., 24
        offset_hour: time offset in hours, for example, if this is 3, then it starts from 12-3=9 p.m. instead of 12 a.m.

    Output:
        start_d: a pandas DatetimeIndex object, indicating the list of starting times
        end_d: a pandas DatetimeIndex object, indicating the list of ending times
    """
    offset_d = pd.Timedelta(offset_hours, unit="h")
    start_d = pd.DatetimeIndex(data=start_date_str_list, tz="US/Eastern") - offset_d
    end_d = start_d + pd.Timedelta(duration, unit="h")
    return (start_d, end_d)


def generate_metadata(start_d, end_d, url_partition=4, img_size=540, redo=0,
        prefix="banana_", add_smell=True, lat="40.42532", lng="-79.91643", zoom="9.233", credits="CREATE Lab",
        category="Plume Viz", name_prefix="PARDUMP ", file_path="https://cocalc-www.createlab.org/test/"):
    """
    Generate the EarthTime layers and the thumbnail server urls that can be called later to obtain video frames

    Input:
        start_d: a pandas DatetimeIndex object, indicating the list of starting times
        end_d: a pandas DatetimeIndex object, indicating the list of ending times
        url_partition: the number of partitions for the thumbnail server request for getting images of video frames
        img_size: the size of the output video (e.g, 540 means 540px for both width and height)
        redo: this is a number to force the server to avoid using the cached file
        prefix: a string prefix for the generated unique share url identifier in the EarthTime layers
        add_smell: a flag to control if you want to add the smell reports to the visualization
        lat: a string that indicates the latitude of the EarthTime base map
        lng: a string that indicates the longitude of the EarthTime base map
        zoom: a string that indicates the zoom level of the EarthTime base map
        credits: a string to fill out the "Credits" column in the output EarthTime layers file
        category: a string to fill out the "Category" column in the output EarthTime layers file
        name_prefix: a string predix for the "Name" column in the output EarthTime layers file
        file_path: an URL path to indicate the location of your hysplit bin files

    Output:
        df_layer: the pandas dataframe for the EarthTime layer document
        df_share_url: the pandas dataframe for the share urls
        df_img_url: the pandas dataframe for the thumbnail server urls to get images of video frames
        file_name: a list of file names that are used for saving the hysplit bin files
    """
    if url_partition < 1:
        url_partition = 1
        print("Error! url_partition is less than 1. Set the url_partition to 1 to fix the error.")

    # Create rows in the EarthTime layer document
    df_template = pd.read_csv("data/earth_time_layer_template.csv")
    df_layer = pd.concat([df_template]*len(start_d), ignore_index=True)
    file_name = prefix + end_d.strftime("%Y%m%d")
    start_d_utc = start_d.tz_convert("UTC")
    end_d_utc = end_d.tz_convert("UTC")
    df_layer["Start date"] = start_d_utc.strftime("%Y%m%d%H%M%S")
    df_layer["End date"] = end_d_utc.strftime("%Y%m%d%H%M%S")
    df_layer["Share link identifier"] = file_name
    df_layer["Name"] = name_prefix + end_d.strftime("%Y-%m-%d")
    df_layer["URL"] = file_path + file_name + ".bin"
    df_layer["Category"] = category
    df_layer["Credits"] = credits

    # Create rows of share URLs
    et_root_url = "https://headless.earthtime.org/#"
    et_part = "v=%s,%s,%s,latLng&ps=2400&startDwell=0&endDwell=0" % (lat, lng, zoom)
    ts_root_url = "https://thumbnails-earthtime.cmucreatelab.org/thumbnail?"
    ts_part = "&width=%d&height=%d&format=zip&fps=30&tileFormat=mp4&startDwell=0&endDwell=0&fromScreenshot&disableUI&redo=%d" % (img_size, img_size, redo)
    share_url_ls = [] # EarthTime share urls
    dt_share_url_ls = [] # the date of the share urls
    img_url_ls = [] # thumbnail server urls
    dt_img_url_ls = [] # the date of the thumbnail server urls

    # NOTE: this part is for testing the new features that override the previous ones
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
        if add_smell:
            l = "l=bdrk_detailed,smell_my_city_pgh_reports_top," + file_name[i] + "&"
        else:
            l = "l=bdrk_detailed," + file_name[i] + "&"
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


def simulate(start_time_eastern, o_file, sources, emit_time_hrs=1, duration=24, filter_ratio=0.8,
        hysplit_root="/projects/hysplit/"):
    """
    Run the HYSPLIT simulation

    Input:
        start_time_eastern: for different dates, use format "2020-03-30 00:00"
        o_file: file path to save the simulation result, e.g., "/projects/cocalc-www.createlab.org/pardumps/test.bin"
        sources: location of the sources of pollution, in an array of DispersionSource objects
        emit_time_hrs: affects the emission time for running each Hysplit model
        duration: total time (in hours) for the simulation, use 24 for a total day, use 12 for testing
        filter_ratio: the ratio that the points will be dropped (e.g., 0.8 means dropping 80% of the points)
        hysplit_root: the root directory of the hysplit software
    """
    print("="*100)
    print("="*100)
    print("start_time_eastern: %s" % start_time_eastern)
    print("o_file: %s" % o_file)

    # Check and make sure that the o_file path is created
    check_and_create_dir(o_file)

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
        if not findInFolder(folder,"PARDUMP*.txt"):
            pdump = findInFolder(folder, "PARDUMP.*")
            # TODO: unzip PARDUMP gzip files
            cmd = hysplit_root + "exec/par2asc -i%s -o%s" % (pdump, pdump+".txt")
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
        # TODO: gzip PARDUMP.* files


def is_url_valid(url):
    """Check if the url is valid (has something)"""
    try:
        r = requests.head(url)
        return r.status_code == requests.codes.ok
    except Exception:
        traceback.print_exc()
        return False


def simulate_worker(start_time_eastern, o_file, sources, emit_time_hrs, duration, filter_ratio, o_url):
    """
    The parallel worker for hysplit simulation

    Input:
        o_url: if not None, check if the URL for the particle file already exists in the remote server
        (for other input parameters, see the docstring of the simulate function)
    """
    # Skip if the file exists in local
    if os.path.isfile(o_file):
        print("File exists in local %s" % o_file)
        return True

    # Skip if the file exists in remote
    if o_url is not None and is_url_valid(o_url):
        print("File exists in remote %s" % o_url)
        return True

    # Perform HYSPLIT model simulation
    try:
        simulate(start_time_eastern, o_file, sources,
                emit_time_hrs=emit_time_hrs, duration=duration, filter_ratio=filter_ratio)
        return True
    except Exception:
        print("-"*60)
        print("Error when creating %s" % o_file)
        traceback.print_exc()
        print("-"*60)
        return False


def get_frames(df_img_url, dir_p="data/rgb/", num_try=0, num_workers=4):
    """
    Call the thumbnail server to generate and get video frames, then save the video frames

    Input:
        df_img_url: the pandas dataframe generated by using the generate_metadata function
        dir_p: the folder path for saving the files
        num_try: the number of times that the function has been called
        num_workers: the number of workers to download the frames (do not use more than 4)
    """
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
    pool = Pool(num_workers)
    result = pool.starmap(urlretrieve_worker, arg_list)
    pool.close()
    pool.join()
    for r in result:
        if r: num_errors += 1
    if num_errors > 0:
        print("="*60)
        print("Has %d errors. Need to do again." % num_errors)
        num_try += 1
        get_frames(df_img_url, num_try=num_try)
    else:
        print("DONE")


def urlretrieve_worker(url, file_p):
    """
    The worker for getting the video frames

    Input:
        url: the url for getting the frames
        file_p: the path for saving the file
    """
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
    except Exception:
        traceback.print_exc()
        error = True
    return error


def check_and_create_dir(path):
    """Check if a directory exists, if not, create it"""
    if path is None: return
    dir_name = os.path.dirname(path)
    if dir_name != "" and not os.path.exists(dir_name):
        try: # this is used to prevent race conditions during parallel computing
            os.makedirs(dir_name)
            os.chmod(dir_name, 0o777)
        except Exception:
            traceback.print_exc()


def unzip_and_rename(in_dir_p, out_dir_p, offset_hours=3):
    """
    Unzip the video frames and rename them to the correct datetime

    Input:
        in_dir_p: path to the folder that has the zip file for one day's data
        out_dir_p: path to the folder that will store the output frames
        offset_hour: time offset in hours, for example, if this is 3, then it starts from 12-3=9 p.m. instead of 12 a.m.
    """
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
            assert(num_files_per_partition - 1 > 0),"Number of files per partition needs to be more than 1"
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


def del_dir(dir_p):
    """Delete a directory and all its contents"""
    if not os.path.isdir(dir_p): return
    try:
        shutil.rmtree(dir_p)
    except Exception:
        traceback.print_exc()


def get_all_file_names_in_folder(path):
    """Return a list of all files in a folder"""
    return [f for f in listdir(path) if isfile(join(path, f))]


def get_all_dir_names_in_folder(path):
    """Return a list of all directories in a folder"""
    return [f for f in listdir(path) if isdir(join(path, f))]


def create_video(in_dir_p, out_file_p, font_p, fps=30, reduce_size=False):
    """
    Add caption to the images by its file name (epochtime), then merge these images into a video

    Input:
        in_dir_p: path to the folder that contains video frames
        out_file_p: the path to the file that will store the video
        font_p: the path to the font file for the caption
        reduce_size: if True, will reduce file size using ffmpeg
    """
    print("Process images in %r" % in_dir_p)
    check_and_create_dir(out_file_p)
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



def get_file_list_from_url(url, ext=""):
    """
    Get a list of file names from an URL
    Input:
        url: an URL path, e.g., "https://cocalc-www.createlab.org/pardumps/plumeviz/video/"
        ext: the extension of the files that you want, e.g., "mp4"
    """
    file_list= None
    try:
        print("\t{Request} %s\n" % url)
        response = urllib.request.urlopen(url)
        soup = BeautifulSoup(response.read(), "html.parser")
        file_list = [url + node.get("href") for node in soup.find_all("a") if node.get("href").endswith(ext)]
        print("\t{Done} %s\n" % url)
    except Exception:
        traceback.print_exc()
    return file_list


def generate_plume_viz_json(video_url):
    """
    Generate a json file for the front-end website
    Input:
        video_url: url path for storing the videos, e.g., "https://cocalc-www.createlab.org/pardumps/plumeviz/video/"
    """
    print("Generate the json file for the front-end...")

    # Get the list of available plume videos
    url_list = get_file_list_from_url(video_url, ext="mp4")
    time_string = [url.split("/")[6].split(".")[0] for url in url_list]
    date_obj = [datetime.datetime.strptime(t, "%Y%m%d") for t in time_string]

    # Get the number of smell reports in the desired dates
    start_time = int(min(date_obj).timestamp() - 86400)
    end_time = int(max(date_obj).timestamp() + 86400)
    smell_pgh_api_url = "https://api.smellpittsburgh.org/api/v2/smell_reports?group_by=day&aggregate=true&smell_value=3%2C4%2C5&start_time=" + str(start_time) + "&end_time=" + str(end_time) + "&state_ids=1&timezone_string=America%252FNew_York"
    smell_counts = None
    try:
        print("\t{Request} %s\n" % smell_pgh_api_url)
        response = urllib.request.urlopen(smell_pgh_api_url)
        smell_counts = json.load(response)
        print("\t{Done} %s\n" % smell_pgh_api_url)
    except Exception:
        traceback.print_exc()

    # Create the json object (for front-end)
    date_obj = pd.to_datetime(date_obj)
    gp_year = date_obj.groupby(date_obj.year)
    viz_json = {}
    for k in gp_year:
        g_json = {"columnNames": ["label", "color", "url", "date"], "data": []}
        for d in sorted(gp_year[k].to_pydatetime()):
            label = d.strftime("%b %d")
            ck = d.strftime("%Y-%m-%d")
            color = -1 if smell_counts is None or ck not in smell_counts else smell_counts[ck]
            url = video_url + d.strftime("%Y%m%d") + ".mp4"
            g_json["data"].append([label, color, url, d.strftime("%Y-%m-%d")])
        viz_json[k] = g_json

    # Save the json for the front-end visualization website
    p = "data/plume_viz.json"
    with open(p, "w") as f:
        json.dump(viz_json, f)
    os.chmod(p, 0o777)
