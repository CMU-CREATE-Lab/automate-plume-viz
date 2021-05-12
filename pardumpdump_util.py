"""
This is the utility function for automating plume visualization
This code was taken and edited from the following path on the hal21 server:
    /projects/earthtime/files/air-src/linRegModel/pardump_example/pardumpdump-randy-amy-util.ipynb
"""


import glob, os, array, datetime, math, random
import numpy as np
from utils import subprocess_check, SimpleProcessPoolExecutor


def gunzipFiles(fnames, zipfnames):
    for fname in zipfnames:
        if fname[:-3] not in fnames:
            print("Unzipping %s" % fname)
            cmd = "gunzip -k %s" % (fname)
            subprocess_check(cmd)


def cleanup(fnames):
    for fname in fnames:
        if fname.find("gz") != -1:
            print("Skipping %s" % (fname))
            continue
        elif fname.find("txt") != -1:
            print("Deleting %s" % (fname))
            cmd = "rm %s" % (fname)
            subprocess_check(cmd)


def findInFolder(folder, pattern):
    result = glob.glob(os.path.join(folder,pattern))
    if len(result) > 0:
        return result[0]
    else:
        return False


def parse_pardump(fname, rgb, filter_ratio=0.8, with_size=False):
    # Process lines in the file
    print("Process lines...")
    points = {}
    with open(fname, "r") as f:
        for line in f:
            try:
                L = []
                raw_line = line.rstrip().split(" ")
                for l in raw_line:
                    if l.rstrip() != '':
                        if l.find('.') > -1:
                            l = float(l)
                        else:
                            l = int(l)
                        L.append(l)
            except:
                L = line
            if len(L) == 7:
                minute = L[6]
                dt = datetime.datetime(2000 + L[2], L[3], L[4], L[5], L[6])
                epoch = datetime_to_epoch(dt)
            elif len(L) == 6:
                x, y = lonlat_to_pixel_xy((L[1], L[0]))
                z = float(L[2])
                if with_size:
                    sigh = sigh_to_pixel(float(l[3]), l[0])
            elif len(L) == 5:
                # This is where we can find the points
                if random.random() > filter_ratio:
                    idx = L[4]
                    if idx not in points:
                        points[idx] = []
                    if minute % 5 == 0:
                        if with_size:
                            points[idx].append([x, y, z, epoch, sigh])
                        else:
                            points[idx].append([x, y, z, epoch])
    # Process points
    print("Process %d points obtained from the file" % len(points))
    c = pack_color(rgb)
    data = []
    for idx in points:
        p = points[idx]
        if len(p) > 1:
            for i in range(0, len(p) - 1):
                p0 = p[i]
                p1 = p[i+1]
                # Each shader record in float32 is:
                # x0, y0, z0, epoch0, x1, y1, z1, epoch1, packedColor
                # x and y are in web mercator space 0,0 is NW 255,255 is SE
                if with_size:
                    data += [p0[0], p0[1], p0[2], p0[3], p1[0], p1[1], p1[2], p1[3], c, p1[4]]
                else:
                    data += [p0[0], p0[1], p0[2], p0[3], p1[0], p1[1], p1[2], p1[3], c]
    return data


def pack_color(color):
    return color[0] + color[1] * 256.0 + color[2] * 256.0 * 256.0;


def lonlat_to_pixel_xy(lonlat):
    (lon, lat) = lonlat
    x = (lon + 180.0) * 256.0 / 360.0 #converts [-180,180] to [0,256]
    y = 128.0 - math.log(math.tan((lat + 90.0) * math.pi / 360.0)) * 128.0 / math.pi
    return [x, y]


def sigh_to_pixel(sigh,lat):
    return sigh / (157000.0 * abs(math.cos(lat)))


def datetime_to_epoch(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def scale_particle(sigh):
    return ((sigh + 1.0) / 12600.0) + 1.0 #offset by 1 to make sure all points draw


def create_bin(fnames, o_file, colormap):
    """Coloring based on hour"""
    i = 0
    step = 255/(len(fnames) - 1)
    points = []
    for fname in fnames:
        rgb = colormap[0][int(i*step)]
        print("Process %s" % fname)
        points += parse_pardump(fname, rgb)
        lines = []
        i += 1
    array.array('f', points).tofile(open(o_file, 'wb'))
    points = []


def create_multisource_bin(fnames, o_file, numSources, with_size, cmaps, duration, filter_ratio=0.8):
    """
    Coloring based on source
    filter_ratio=0.8 means that 80% of the points will be dropped
    with_size=True means visualizing puffs instead of particles
    """
    runTimeHrs = len(fnames) / numSources
    print("Only use %.2f of all the points to reduce file size" % (1-filter_ratio))
    all_points = []
    
    maxWorkers = 10
    pool = SimpleProcessPoolExecutor(maxWorkers)
    for i, fname in enumerate(fnames):
        src = i / runTimeHrs
        rgb = cmaps[numSources - 1][int(src)]
        pool.submit(parse_pardump,fname,rgb,filter_ratio=filter_ratio,with_size=with_size)
        #
    all_points = pool.shutdown()
    #
    points = np.concatenate(all_points)
    attrs = 10 if with_size else 9
    points = points.reshape((-1, attrs))
    np.random.shuffle(points)
    points = points.reshape((-1, 1))
    print("Writing array to file %s" % o_file)
    array.array('f', points).tofile(open(o_file, 'wb'))
