"""
This is the utility function for automating plume visualization
Developed by Amy, Randy, and edited by Yen-Chia
"""
import glob, os
import numpy as np


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


def parse_pardump(fname):
    n_lines = sum(1 for i in open(fname, "rb"))
    # Knowing the number of lines can speed up the process
    # using the append method is slow
    lines = [None]*n_lines
    with open(fname, "rb") as f:
        i = 0
        for line in f:
            try:
                cooked_line = []
                raw_line = line.rstrip().split(" ")
                for l in raw_line:
                    if l.rstrip() != '':
                        if l.find('.') > -1:
                            l = float(l)
                        else:
                            l = int(l)
                        cooked_line.append(l)
                lines[i] = cooked_line
            except:
                lines[i] = line
            i += 1
    return lines


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


# flow points, assumes time is every minute
# Output points format is:
def get_points(lines, rgb, filtered=0.0):
    points = {}
    c = pack_color(rgb)
    for l in lines:
        if len(l) == 7:
            minute = l[6]
            dt = datetime.datetime(2000 + l[2],l[3],l[4],l[5],l[6])
            epoch = datetime_to_epoch(dt)
        if len(l) == 6:
            x,y = lonlat_to_pixel_xy((l[1],l[0]))
            z = float(l[2])
        if len(l) == 5:
            idx = l[4]
            if idx not in points:
                points[idx] = []
            if minute % 5 == 0:
                points[idx].append([x,y,z,epoch])
    data = []
    for idx in points:
        if random.random() > filtered:
            p = points[idx]
            if len(p) > 1:
                for i in range(0,len(p) - 1):
                    p0 = p[i]
                    p1 = p[i+1]
                    # Each shader record in float32 is:
                    # x0, y0, z0, epoch0, x1, y1, z1, epoch1, packedColor
                    # x and y are in web mercator space 0,0 is NW 255,255 is SE
                    data += [p0[0],p0[1], p0[2], p0[3], p1[0],p1[1], p1[2], p1[3], c]
    return data


def get_points_with_size(lines, rgb, filtered=0.0):
    points = {}
    c = pack_color(rgb)
    for l in lines:
        if len(l) == 7:
            minute = l[6]
            dt = datetime.datetime(2000 + l[2],l[3],l[4],l[5],l[6])
            epoch = datetime_to_epoch(dt)
        if len(l) == 6:
            x,y = lonlat_to_pixel_xy((l[1],l[0]))
            z = float(l[2])
            sigh = sigh_to_pixel(float(l[3]),l[0])
        if len(l) == 5:
            idx = l[4]
            if idx not in points:
                points[idx] = []
            if minute % 5 == 0:
                points[idx].append([x,y,z,epoch,sigh])
    data = []
    for idx in points:
        if random.random() > filtered:
            p = points[idx]
            if len(p) > 1:
                for i in range(0,len(p) - 1):
                    p0 = p[i]
                    p1 = p[i+1]
                    # Each shader record in float32 is:
                    # x0, y0, z0, epoch0, x1, y1, z1, epoch1, packedColor
                    # x and y are in web mercator space 0,0 is NW 255,255 is SE
                    data += [p0[0],p0[1], p0[2], p0[3], p1[0],p1[1], p1[2], p1[3], c, p1[4]]
    return data


# coloring based on hour
def create_bin(fnames, o_file):
    i = 0
    step = 255/(len(fnames) - 1)
    points = []
    for fname in fnames:
        rgb = colormap[0][int(i*step)]
        print("Process %s" % fname)
        lines = parse_pardump(fname)
        points += get_points(lines, rgb)
        lines = []
        i += 1
    array.array('f', points).tofile(open(o_fname, 'wb'))
    points = []


# coloring based on source
def create_multisource_bin(fnames, o_file, numSources, with_size, cmaps, duration):
    if with_size: # if you're visualizing puffs instead of particles
        pointGetter = get_points_with_size
        attrs = 10
    else:
        pointGetter = get_points
        attrs = 9
    points = np.array([])
    runTimeHrs = len(fnames) / numSources
    # Randomly filter out points proportional to number of sources to improve loading speed
    filteredPoints = (1 - 1.0 / numSources) * (1.0 / (duration / 8.0))
    for i,fname in enumerate(fnames):
        src = i / runTimeHrs
        rgb = cmaps[numSources - 1][int(src)]
        print("Read lines in %s" % fname)
        lines = parse_pardump(fname)
        # Use pointGetter
        print("Get points from %d lines..." % len(lines))
        points = np.append(points, pointGetter(lines, rgb, filteredPoints))
        lines = []
    points = points.reshape((-1,attrs))
    np.random.shuffle(points)
    points = points.reshape((-1,1))
    print("Writing array to file %s" % o_file)
    array.array('f', points).tofile(open(o_file, 'wb'))
