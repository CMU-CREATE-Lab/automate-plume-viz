"""
This is the utility function for automating plume visualization
This code was taken and edited from the following path on the hal21 server:
    /projects/earthtime/files/air-src/linRegModel/pardump_example/pardumpdump-randy-amy-util.ipynb
"""


import glob, os, array, datetime, dateutil.parser, math, random, re, json
import numpy as np
import pandas as pd

from utils import subprocess_check, SimpleProcessPoolExecutor

#adjust epoch to January 1, 2020 and scale from seconds to minutes for greater precision
EPOCH_OFFSET = 1577836800
EPOCH_SCALE = 60

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

def lonlat_to_pixel_xy_series(lonlat):
    (lon, lat) = lonlat
    x = (lon + 180.0) * 256.0 / 360.0 #converts [-180,180] to [0,256]
    y = 128.0 - np.log(np.tan((lat + 90.0) * math.pi / 360.0)) * 128.0 / math.pi
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


def create_multisource_bin(fnames, o_file, numSources, cmaps, filter_out_ratios=0.8):
    """
    Coloring based on source
    filter_out_ratios=0.8 means that 80% of the points will be dropped. if specified as a dict, filter ratios are applied per source.
    with_size=True means visualizing puffs instead of particles
    """
    runTimeHrs = int(len(fnames) / numSources)

    filter_dict = False
    if type(filter_out_ratios) == list:
        assert len(filter_out_ratios) == numSources
        filter_dict = True

    print("Only use %s of all the points to reduce file size" % filter_out_ratios)
    maxWorkers = 10
    pool = SimpleProcessPoolExecutor(maxWorkers)
    for i in range(numSources):
        start_file = i * runTimeHrs
        single_source = fnames[start_file:start_file+runTimeHrs]
        rgb = cmaps[i]
        filter_out = filter_out_ratios[i] if filter_dict else filter_out_ratios

        pool.submit(particle_dat_to_bin,single_source,rgb,filter_out)
    
    all_points = pool.shutdown()
    points = np.concatenate(all_points)

    #construct subset dir

    #sort by first timestamp
    points = points[points[:,3].argsort()]
    tstamps = points[:,3]

    subsets = []
    first = int(min(tstamps))
    last = int(max(tstamps))
    last_index = 0

    for t in range(first + 60,last,60):
        subset_record = {}
        next_index = np.where(tstamps == t)[0][0]
        subset_record["epoch"] = int(datetime.datetime.fromtimestamp(tstamps[next_index] * EPOCH_SCALE + EPOCH_OFFSET).timestamp())
        subset_record["first"] = int(last_index)
        subset_record["count"] = int(next_index - last_index)
        
        subsets.append(subset_record)
        last_index = next_index
    
    with open(o_file[:-4] + ".json", 'w') as f:
        json.dump(subsets, f)
    
    print("Writing array to file %s" % o_file)
    points.tofile(o_file)
    print("Zipping bin file %s" % o_file)
    cmd = "pigz -9 %s" % (o_file)
    subprocess_check(cmd)
    print("Successfully zipped %s" % o_file)


def read_particle_dat(particle_dat_filename, filter_out = .5, subsample=10 ):
    pardump_df = pd.read_csv(particle_dat_filename, delim_whitespace=True)
    print(f'Read {len(pardump_df)} records')

    pardump_df.sort_values(['index', 'time'], inplace=True)
    min_times = pardump_df.groupby(by='index')['time'].min()
    pardump_df = pd.merge(left=pardump_df,right=min_times,on='index',suffixes=['','_min'])
    pardump_df = pardump_df[pardump_df['time'].mod(subsample) == pardump_df['time_min'].mod(subsample)]

    print(f'{len(pardump_df)} records after subsampling time by {subsample}')

    assert(filter_out < 1)
    keep_ratio = 1 - filter_out
    num_particles = pardump_df['index'].max()
    keep_indxs = random.sample(range(1,num_particles),int(num_particles * keep_ratio))
    pardump_df = pardump_df[pardump_df['index'].isin(keep_indxs)]
    print(f'{len(pardump_df)} records after dropping {filter_out * 100}% of particle indices')

    pardump_df['time'] = pardump_df['time'] * 60
    start_datetime = dateutil.parser.parse(re.search(
        r'\d{8}_\d{6}-\d{4}',particle_dat_filename).group(0).replace('_',' ')).timestamp()
    pardump_df['time'] = pardump_df['time'] + start_datetime
    print(start_datetime)
    pardump_df.index = range(0, len(pardump_df))
    return pardump_df

def read_and_concat_particle_dat_files(filenames,filter_out):
    dfs = []
    index_offset = 0

    for filename in filenames:
        df: pd.DataFrame = read_particle_dat(filename,filter_out)
        df['index'] += index_offset
        print(f'Read {len(df)} records, indices {df["index"].min()} to {df["index"].max()}, from {filename}')
        index_offset = df['index'].max()
        dfs.append(df)

    df = pd.concat(dfs)
    print(f'Concatenated to {len(df)} records, indices {df["index"].min()} to {df["index"].max()}')
    return df

def df_to_bin(pardump_df, rgb):
    index = pardump_df['index'].to_numpy()
    lat = pardump_df['lat'].to_numpy()
    lon = pardump_df['lon'].to_numpy()
    alt = pardump_df['agl'].to_numpy()
    epochtime = ((pardump_df['time'] - EPOCH_OFFSET) / EPOCH_SCALE ).to_numpy()
    packed_color = pack_color(rgb)

    x, y = lonlat_to_pixel_xy_series((lon, lat))

    newdf = pd.DataFrame({'particle_id0':index[:-1],'x0': x[:-1], 'y0': y[:-1], 'z0': alt[:-1], 't0': epochtime[:-1],
                         'particle_id1':index[1:],'x1': x[1:], 'y1': y[1:], 'z1': alt[1:], 't1': epochtime[1:],
                         'color':packed_color})
    records = newdf[newdf.particle_id0 == newdf.particle_id1]
    return records.drop(['particle_id0','particle_id1'],axis=1).to_numpy(np.float32)

def particle_dat_to_bin(filenames,rgb,filter_out):
    df = read_and_concat_particle_dat_files(filenames,filter_out)
    df.reset_index(inplace=True)
    return df_to_bin(df,rgb)
