"""
This code was taken and edited from the following path on hal21 server (on Oct 26, 2020):
    /projects/earthtime/files/air-src/linRegModel/cachedHysplitRunLib.ipynb
"""


import sys, datetime, dateutil, enum, hashlib, io, os, threading, traceback, glob, gzip, shutil, subprocess
from jinja2 import Template
from filelock import FileLock
from utils import SimpleThreadPoolExecutor, download_file, subprocess_check
import pandas as pd
import numpy as np


def parse_eastern(date):
    easternTZ = dateutil.tz.gettz('America/New_York')
    return dateutil.parser.parse(date).replace(tzinfo=easternTZ)


class InitdModelType(enum.Enum):
    """
    The menu is divided into two sections.
    In each case the value of the INITD namelist parameter is being set.
    In the upper portion of the menu,
    ...the model is configured as either a full 3D particle or puff model,
    ...or some hybrid combination of the two.
    The released particles or puffs maintain their mode for the entire duration of the simulation.
    Valid options are:
        0 - 3D particle horizontal and vertical (DEFAULT)
        1 - Gaussian-horizontal and Top-Hat vertical puff (Gh-THv)
        2 - Top-Hat-horizontal and vertical puff (THh-THv)
        3 - Gaussian-horizontal puff and vertical particle distribution (Gh-Pv)
        4 - Top-Hat-horizontal puff and vertical particle distribution (THh-Pv)
    Introduced with the September 2004 version are mixed mode model calculations,
    ...where the mode can change during transport depending upon the age (from release) of the particle.
    A mixed-mode may be selected to take advantage of the more accurate representation of
    ...the 3D particle approach near the source and the smoother horizontal distribution
    ...provided by one of the hybrid puff approaches at the longer transport distances.
    In a long-range or regional puff simulation,
    ...where the concentration grid may be rather coarse,
    ...puffs may pass between concentration sampling nodes during the initial stages of the transport,
    ...a stage when the plume is still narrow.
    Using mode #104 would start the simulation with particles (and concentration grid cells)
    ...and then switch to puff mode (and concentration sampling nodes)
    ...when the particles are distributed over multiple concentration grid cells.
    Valid options are:
        103 - 3D particle (#0) converts to Gh-Pv (#3)
        104 - 3D particle (#0) converts to THh-Pv (#4)
        130 - Gh-Pv (#3) converts to 3D particle (#0)
        140 - THh-Pv (#4) converts to 3D particle (#0)
        109 - 3D particle converts to grid (global model)
    Hysplit online defaults to mode 104 (auto-switch from 0 to 4)
    Hysplit manual says default is 0
    """
    ParticleHV = 0
    GaussianH_TopHapV = 1
    TopHatHV = 2
    GaussianH_ParticleV = 3
    TopHatH_ParticleV = 4
    ParticleHV_to_GaussianH_ParticleV = 103
    ParticleHV_to_TopHatH_ParticleV = 104
    GaussianH_ParticleV_to_ParticleHV = 130
    TopHatH_ParticleV_to_ParticleHV = 140
    ParticleHV_to_Grid = 109


class HysplitModelSettings:
    def __init__(self,
                 initdModelType=InitdModelType.ParticleHV,
                 hourlyPardump=False):
        if isinstance(initdModelType, int):
            print(
                'Consider using InitdModelType enum for HysplitModelSettings')
            initdModelType = InitdModelType(int)
        self.initdModelType = initdModelType
        self.hourlyPardump = hourlyPardump

    def __str__(self):
        ret = '<HMS'
        ret += ' initd=%d' % self.initdModelType.value
        if self.hourlyPardump:
            pardumpMins = 60
        else:
            pardumpMins = 1
        ret += ' pardump=%dm' % pardumpMins
        ret += '>'
        return ret


class DispersionSource:
    # minHeight and maxHeight are in meters
    # area is in square meters
    def __init__(self, name, lat, lon, minHeight, maxHeight, areaSqM=0):
        assert(isinstance(name, str))
        self.name = name

        assert(-90 <= lat and lat <= 90)
        self.lat = round(lat, 6)

        assert(-180 <= lon and lon <= 180)
        self.lon = round(lon, 6)

        assert(0 <= minHeight and minHeight <= maxHeight)
        self.minHeight = minHeight

        assert(maxHeight <= 1000)
        self.maxHeight = maxHeight

        assert(areaSqM >= 0)
        self.areaSqM = areaSqM

    def cachePath(self):
        path = '%.6f,%.6f_%g-%g' % (self.lat, self.lon, self.minHeight, self.maxHeight)
        if self.areaSqM > 0:
            path += '_%g' % self.areaSqM
        return path

    def __repr__(self):
        return self.name


assert(DispersionSource(name='Test', lat=40.123456789, lon=-79.123456789,
    minHeight=10, maxHeight=50).cachePath() == '40.123457,-79.123457_10-50')
assert(DispersionSource(name='Test', lat=40.123456789, lon=-79.123456789,
    minHeight=10, maxHeight=50, areaSqM=100).cachePath() == '40.123457,-79.123457_10-50_100')


class CachedDispersionRun:
    """
    Input:
        source -- DispersionSource representing lat/lon and altitude min/max of emission source
        runStartLocal -- beginning of emission, in local timezone
        emitTime -- length of emission, in hours
        runTime -- length of simulation, in hours
        fileName -- file name of binary results file
        hysplit_root -- the root directory of the hysplit software
    """
    def __init__(self, source, runStartLocal, emitTimeHrs, runTimeHrs, hysplitModelSettings,
                 fileName='cdump', hysplit_root='/projects/hysplit/', verbose=False,
                 dispersionCachePath='/projects/earthtime/air-src/linRegModel/dispersionCache',
                 hrrrDirPath='/projects/earthtime/air-data/hrrr'):
        try:
            self.dispersionCachePath = dispersionCachePath
            self.hrrrDirPath = hrrrDirPath

            assert(source)
            self.source = source

            assert(runStartLocal)
            self.runStartLocal = runStartLocal
            self.runStartUtc = runStartLocal.astimezone(dateutil.tz.tzutc())

            msg = 'CachedDispersionRun start=%s' % self.runStartLocal
            if self.runStartLocal.tzinfo != self.runStartUtc.tzinfo:
                msg += ' (%s)' % (self.runStartUtc)

            msg += ' emitTime=%dh runTime=%dh initdModelType=%s source=%s' % (
                    emitTimeHrs, runTimeHrs, repr(hysplitModelSettings.initdModelType), source)

            assert(emitTimeHrs)
            self.emitTimeHrs = emitTimeHrs

            assert(runTimeHrs)
            self.runTimeHrs = int(runTimeHrs)

            self.initdModelType = hysplitModelSettings.initdModelType

            self.verbose= verbose
            if self.verbose:
                sys.stdout.write(msg + '\n')

            self.logfile = None

            self.hourlyPardump = hysplitModelSettings.hourlyPardump

            if not os.path.exists(self.path()) and self.hourlyPardump:
                self.hourlyPardump = False
                if os.path.exists(self.path()):
                    if self.verbose:
                        sys.stdout.write('CachedDispersionRun -- found minutely pardump version, overriding hourlyPardump to be False\n')
                else:
                    self.hourlyPardump = True
        except AssertionError:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]

            print('An error occurred on line {} in statement {}'.format(line, text))
            exit(1)

        self.fileName = fileName
        self.hysplitLoc = hysplit_root + "exec/"
        self.runHr = int(runTimeHrs)
        self.runMin = int((runTimeHrs - int(runTimeHrs))*60)
        self.fNames = self.fetchWeatherFiles()

    def assertComplete(self):
        """Assert this run has all files associated with successful completion, e.g. cdump"""
        errs = []
        if not os.path.exists(self.cdumpPath()):
            errs.append('cdump file %s should exist, but does not' % self.cdumpPath())
        if errs:
            raise Exception('Errors found in HYSPLIT directory %s: %s' % (self.path(), '; '.join(errs)))

    def findOrRun(self):
        if not os.path.exists(self.path()):
            self.run()
            self.assertComplete()
        else:
            self.vlog('Hysplit run at location %s already complete.' % self.path())
            # Force uncompressing of PARDUMP
            # Randy commented Feb 22 ... do we still need this here or can we move to where we actually need to read PARDUMP?
            # self.getUncompressedPardump()
            self.assertComplete()
        return(self.path())

    def run(self):
        """
        Called internally.
        Make sure we aren't already completed or in progress somewhere else before calling this method?
        """
        # Short circuit if already done
        if os.path.exists(self.path()):
            return(self.path())

        # Create parent directory of lock
        # If two threads run concurrently, allow this call to fail silently
        os.makedirs(os.path.dirname(self.path()),exist_ok=True)

        # Create and hold lockfile
        lockfilePath = self.path() + '.lock'
        lock = FileLock(lockfilePath)
        with lock:
            # Delete old temp directory if it exists (need to delete tmpPaths with different pids)
            if os.path.exists(self.tmpPath()):
                self.log('Deleting old temp directory %s' % self.tmpPath())
                shutil.rmtree(self.tmpPath())
            # Short circuit if this is the second process to acquire the FileLock, and thus the run is complete
            if os.path.exists(os.path.join(self.path(),'cdump')):
                return(self.path())

            os.makedirs(self.tmpPath())
            self.logfile = open(self.tmpPath() + '/log.txt', 'w')
            self.makeSetup()
            self.makeASC()
            self.makeControl()
            self.vlog('Running dispersion, path %s, settings %s' % (self.tmpPath(), self.settingsAsString()))
            try:
                # TODO: have the HYSPLIT subprocess chdir instead of the python parent
                self.runDispersion()
                self.vlog('SUCCESS for dispersion run: %s' % self.tmpPath())
            except Exception as e:
                self.log('Received exception %s during DispersionRun' % e)
                self.log('Run directory: %s' % self.tmpPath())
                self.log('Settings: %s' % self.settingsAsString())
                raise
            try:
                os.rename(self.tmpPath(), self.path())
                self.vlog('Successful rename from %s to %s' % (self.tmpPath(),self.path()))
            except Exception as e:
                self.log('Received exception %s during rename' % e)
                self.log('Run directory: %s' % self.tmpPath())
                self.log('Settings: %s' % self.settingsAsString())
                raise
        self.assertComplete()

    def log(self, *args, include_stdout=True):
        prefix = '%s %s' % (os.getpid(), threading.get_ident())
        buf = io.StringIO()
        print(prefix, *args, file=buf)
        if include_stdout:
            sys.stdout.write(buf.getvalue())
            sys.stdout.flush()
        if self.logfile:
            self.logfile.write(buf.getvalue())
            self.logfile.flush()

    def vlog(self, *args):
        """Log only to stdout if in verbose mode"""
        self.log(*args, include_stdout=self.verbose)

    def runDispersion(self):
        hyString = self.hysplitLoc + 'hycs_std'
        #out = subprocess.run(hyString, shell=True)
        subprocess_check(hyString, cwd=self.tmpPath(), verbose=True)

    def getUncompressedPardump(self):
        """Get the path to the uncompressed PARDUMP file"""
        # Old archived pardumps should be gzipped. ensure that unzipped pardump is available in folder
        # This looks like it might be wrong. Randy, Yen-Chia 2020-08-24
        pdumps = glob.glob(self.path() + '/PARDUMP.*')
        zipdump = glob.glob(self.path() + '/PARDUMP.*.gz')
        if zipdump:
            if zipdump[0][:-3] not in pdumps:
                with gzip.open(zipdump[0], 'rb') as f_in:
                    with open(zipdump[0][:-3], 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            return zipdump[0][:-3]
        else:
            return pdumps[0]

    def settingsAsString(self):
        ret = '{'
        ret += ','.join(['sourceLoc:[%.6f,%.6f]' % (self.source.lat, self.source.lon),
                         'sourceHeight:[%g,%g]' % (self.source.minHeight, self.source.maxHeight),
                         'runStartLocal:"%s"' % self.runStartLocal,
                         'emitTimeHrs:%g' % self.emitTimeHrs,
                         'runTimeHrs:%g' % self.runTimeHrs,
                         'initdModelType:%s' % repr(self.initdModelType)])
        ret += '}'
        return ret

    def tmpPath(self):
        """Compute temp path unique to this instance of Python, based on PID and thread ID"""
        return '%s_%d_%d.tmp' % (self.path(), os.getpid(), threading.get_ident())

    def localPath(self):
        """Compute cache pathname relative to dispersionCachePath parent"""
        ret = os.path.join(self.source.cachePath(), '%s_%gh_%gh_%g' % (self.runStartLocal.strftime('%Y%m%d_%H%M%S%z'), self.emitTimeHrs, self.runTimeHrs, self.initdModelType.value))
        if self.hourlyPardump:
            # Minutely (P1) pardump is assumed if this field doesn't exists, for backwards compatibility
            ret += '_P60'
        return ret

    def path(self):
        """Compute cache pathname"""
        return os.path.join(self.dispersionCachePath, self.localPath())

    def shortPath(self):
        """
        Some hysplit commands can't cope with long filenames
        Symlink a hashed version of the path in /tmp
        """
        fullPath = self.path()
        linkPath = '/tmp/' + hashlib.sha224(fullPath.encode()).hexdigest()
        if not os.path.exists(linkPath):
            os.symlink(fullPath, linkPath)
        return linkPath

    def settingsPath(self):
        """
        Possibly old code, only used for recognizing multiple runs for visualization
        Don't bother appending '_Pn' since we only run old visualizations
        ...on directories that have full minute-scale pardump
        """
        print('TODO: consider changing visualization code to no longer use settingsPath, and then delete this member fn')
        return os.path.join(self.dispersionCachePath,
            '*/%s*_%gh_*_%g' % (self.runStartLocal.strftime('%Y%m%d'), self.emitTimeHrs, self.initdModelType.value))

    def cdumpPath(self):
        return self.path() + '/cdump'

    def shortCdumpPath(self):
        return self.shortPath() + '/cdump'

    def createConcPlot(self, outputPath, frameno=None, verbose=False):
        """
        outputPath can end in .ps or .png
        frameno is 1 for the first frame, add one for each 15 minutes as we currently run hysplit
        """
        hysplitPath = '/projects/hysplit'
        outputSuffix = os.path.splitext(outputPath)[1]
        convertCmds = []
        if outputSuffix.lower() == '.ps':
            psPath = outputPath
        elif outputSuffix.lower() == '.png':
            psPath = '/tmp/psconvert-%d-%d.ps' % (os.getpid(), threading.get_ident())
            # Compute temp path unique to this instance of Python, based on PID and thread ID
            convertCmds.append([
                    'gmt',
                    'psconvert',
                    '-A',
                    psPath,
                    '-Tg', # PNG format
                    '-F%s' % outputPath
            ])
            # Trim whitespace
            convertCmds.append(['mogrify', '-trim', outputPath])
        cmd = [
            '%s/exec/concplot' % hysplitPath,
            '-i%s' % self.shortCdumpPath(),
            '-o%s' % psPath,
            '-j%s/graphics/arlmap' % hysplitPath
            ]
        fixedConcentrations = True
        if fixedConcentrations:
            # Fix concentration contours in powers of ten from 1e-9 ... 1e-14
            cmd += ['-c4', '-v1E-9+1E-10+1E-11+1E-12+1E-13+1E-14']
        if frameno != None:
            cmd.append('-n%d:%d' % (frameno, frameno))

        subprocess_check(cmd, verbose=verbose)
        for convertCmd in convertCmds:
            subprocess_check(convertCmd, verbose=verbose)
        if len(convertCmds):
            os.unlink(psPath)

    def fetchWeatherFiles(self):
        hrrrDir = os.path.abspath(self.hrrrDirPath)
        fNames = []
        # Date when NOAA archive format changed
        isReformat = self.runStartUtc > datetime.datetime(2019,7,22,0,0,0,0,dateutil.tz.tzutc())
        for dt in self.computeTimes():
            name = dt.strftime('hysplit.%Y%m%d.%Hz.hrrra')
            fullPath = hrrrDir + '/' + name
            if not os.path.exists(fullPath):
                if isReformat:
                    linkEnd = dt.strftime('%Y%m%d_%H-') + str(dt.hour + 5).zfill(2) + '_hrrr'
                    download_file('ftp://arlftp.arlhq.noaa.gov/pub/archives/hrrr/' + linkEnd, fullPath)
                else:
                    download_file('ftp://arlftp.arlhq.noaa.gov/pub/archives/hrrr.v1/' + name, fullPath)
            fNames.append(fullPath)
        return fNames

    def computeTimes(self):
        dtimes = []
        t = self.runStartUtc
        cutoffTime = datetime.datetime.fromtimestamp(int((self.runStartUtc + datetime.timedelta(hours=(self.runTimeHrs+6))).timestamp()/(6*3600))*(6*3600), dateutil.tz.tzutc())
        while(t < cutoffTime):
            dtimes.append(datetime.datetime.fromtimestamp(int(t.timestamp() / (6*3600)) * (6*3600), dateutil.tz.tzutc()))
            t = t + datetime.timedelta(hours = 6)
        return dtimes

    def makeSetup(self):
        """See hysplit users guide section "Particle File Output Options" for ndump and ncycl"""
        if self.hourlyPardump:
            # dump after every 1 hour, cycling every 1 hour because ncycl = 1
            ndump = 1
        else:
            # dump every 1 minute until entire run is done
            ndump = -self.runTimeHrs
        templ = Template(
            """&SETUP
{#          #}NUMPAR = 2500,
{#          #}MAXPAR = 25000,
{#          #}INITD ={{run.initdModelType.value}},
{#          #}CONAGE = 1,
{#          #}KSPL = 1,
{#          #}ndump = {{ndump}},
{#          #}ncycl = 1,
{#          #}delt = 1,
{#          #}poutf = 'PARDUMP.h{{run.runStartLocal.hour}}',
{#          #}/\n""", keep_trailing_newline=1)
        content = templ.render(run=self, ndump=ndump)
        cFile = open(self.tmpPath() + '/SETUP.CFG', 'w')
        cFile.write(content)
        cFile.close()

    def makeASC(self):
        templ = Template(
            """-90.0   -180.0  lat/lon of lower left corner
{#          #}1.0     1.0     lat/lon spacing in degrees
{#          #}180     360     lat/lon number of data points
{#          #}2               default land use category
{#          #}0.2             default roughness length (m)
{#          #}'"""+hysplit_root+"""bdyfiles/'  directory of files""")
        cFile = open(self.tmpPath() + '/ASCDATA.CFG','w')
        cFile.write(templ.render(run=self))
        cFile.close()

    def makeControl(self):
        templ = Template(
            """{{ run.runStartUtc.strftime('%y %m %d %H %M') }} #1: run start time in YY MM DD HH MN (UTC)
{#          #}2 #2: NUMBER OF SOURCE LOCATIONS
{#          #}{% for height in (run.source.minHeight, run.source.maxHeight) -%}
{#          #}{{ run.source.lat }} {{ run.source.lon }} {{ height }} 1 {{ run.source.areaSqM }} #3: SOURCE LATITUDE | LONGITUDE | HEIGHT(m-agl) | EMISSION RATE (per hour) | AREA (sq m)
{#          #}{%- endfor -%}
{#          #}{{ run.runTimeHrs }} #4: TOTAL RUN TIME (hours)
{#          #}0 #5: VERTICAL MOTION (USE MODEL VERTICAL VELOCITY)
{#          #}10000 #6: TOP OF MODEL DOMAIN (m-AGL)
{#          #}{{ len(run.fNames) }} #7: NUMBER OF INPUT DATA GRIDS
{#          #}{% for file in run.fNames -%}
{#          #}{{ os.path.dirname(file) }}/
{#          #}{{ os.path.basename(file) }}
{#          #}{%- endfor -%}
{#          #}1 #10: NUMBER OF DIFFERENT POLLUTANTS
{#          #}TEST #11: POLLUTANT IDENTIFICATION
{#          #}1 #12: EMISSION RATE (per hour)
{#          #}{{run.emitTimeHrs}} #13: HOURS OF EMISSION
{#          #}{{run.runStartUtc.strftime('%y %m %d %H %M')}} #14: EMISSION START TIME: YY mm dd HH MM
{#          #}1 #15: NUMBER OF CONCENTRATION GRIDS
{#          #}0 0 #16: CONC GRID CENTER (LATITUDE LONGITUDE); DEFAULT SOURCE LOC
{#          #}0.003 0.003 #17: CONC GRID SPACING (degrees) LATITUDE LONGITUDE
{#          #}1 1 #18: CONC GRID SPAN (degrees) LATITUDE LONGITUDE
{#          #}./
{#          #}{{run.fileName}}
{#          #}1 #21: NUMBER OF VERTICAL CONCENTRATION LEVELS
{#          #}100 #22: HEIGHT OF EACH CONCENTRATION LEVEL (m-agl)
{#          #}{{run.runStartUtc.strftime('%y %m %d %H %M')}} #23: SAMPLING START TIME:YEAR MONTH DAY HOUR MINUTE
{#          #}00 00 00 {{run.runHr}} {{run.runMin}} #24: SAMPLING STOP TIME:YEAR MONTH DAY HOUR MINUTE
{#          #}0 0 15 #25: SAMPLING INTERVAL: TYPE (AVERAGING) HOUR MINUTE
{#          #}0 #26: NUMBER OF DEPOSITING POLLUTANTS
{#          #}0.0 0.0 0.0 #27: PARTICLE:DIAMETER (um), DENSITY (g/cc), SHAPE
{#          #}0.0 0.0 0.0 0.0 0.0 #28: ATTRIBUTES, ZERO (NO DEPOSITING)
{#          #}0.0 0.0 0.0 #29: WET REMOVAL, ZERO (NO DEPOSITING)
{#          #}0 #30: RADIOACTIVE DECAY HALF-LIFE (days)
{#          #}0.0 #31: POLLUTANT RESUSPENSION""")
        cFile = open(self.tmpPath() + '/CONTROL', 'w')
        cFile.write(templ.render(run=self, os=os, len=len))
        cFile.close()

    def saveToText(self, name):
        """
        -s: Single file output
        -c: output all information from binary (does not work with -s set)
        -m: if -s isn't set, removes column headers from output file
        -t: if -s and -c aren't set, includes minutes in file name
        -v: order lon/lat
        -x: Extended precision
        -z: Include zeros
        """
        hyString = self.hysplitLoc + ('con2asc -i%s -s -t -v -x -z' %name)
        subprocess.run(hyString, cwd=(self.tmpPath()), shell=True)

    def interpolate(self, cdumpFile, outputFile, stationFile):
        #subprocess_check('ls -l %s %s' % (cdumpFile, stationFile), verbose=True)
        #subprocess_check('cat %s' % stationFile, verbose=True)
        hyString = self.hysplitLoc + ('con2stn -i%s -o%s -s%s' %(cdumpFile, outputFile, stationFile))
        #print(hyString)
        subprocess_check(hyString, cwd=(self.path()))
        #print('done interpolation')

    def readInterpFile(self,inFile,sensors):
        """
        Reads in result of hysplit interpolation
        Simplifies into single average timestamp and returns DataFrame
        Input:
            inFile -- path and filename of hysplit interpolation results
        Output:
            interpDat -- DataFrame with index = timestamps, columns = sensor IDs
        """
        #print('readInterFile reading from %s' % inFile)
        #subprocess_check('wc %s' % inFile, verbose=True)
        fIn = pd.read_csv(inFile, header=0, delimiter='\s+')

        timestamps = []

        for ii in np.arange(fIn.shape[0]):
            t1 = datetime.datetime(2000 + fIn['YR'][ii], fIn['MO'][ii], fIn['DA1'][ii], fIn['HR1'][ii], fIn['MN1'][ii], tzinfo=datetime.timezone.utc)
            t2 = datetime.datetime(2000 + fIn['YR'][ii], fIn['MO'][ii], fIn['DA2'][ii], fIn['HR2'][ii], fIn['MN2'][ii], tzinfo=datetime.timezone.utc)
            # Convert timestamp to nanoseconds so it can be converted to pandas datetime64 object
            timestamps.append((t1 + (t2 - t1)/2).timestamp() * 1e9)

        interpDat = pd.DataFrame(data=fIn.iloc[:,9:])
        interpDat.index = pd.to_datetime(timestamps, utc=True).tz_convert(self.runStartLocal.tzinfo)

        sensorIDdict = {str(sensor.id()): sensor for sensor in sensors}
        interpDat = interpDat.rename(columns=sensorIDdict)

        return interpDat


def getDispersionRun(source,runStartLocal,emitTimeHrs,runTimeHrs,hysplitModelSettings,verbose=False,
        dispersionCachePath='/projects/earthtime/air-src/linRegModel/dispersionCache',
        hrrrDirPath='/projects/earthtime/air-data/hrrr'):
    run = CachedDispersionRun(
            source=source,
            runStartLocal=runStartLocal,
            emitTimeHrs=emitTimeHrs,
            runTimeHrs=runTimeHrs,
            hysplitModelSettings=hysplitModelSettings,
            dispersionCachePath=dispersionCachePath,
            hrrrDirPath=hrrrDirPath,
            verbose=verbose
    )
    run.findOrRun()
    run.assertComplete()
    return run


def getMultiHourDispersionRunsParallel(source,runStartLocal,emitTimeHrs,totalRunTimeHrs,
        hysplitModelSettings,backwardsHrs=0,resolutionHrs=1,
        dispersionCachePath='/projects/earthtime/air-src/linRegModel/dispersionCache',
        hrrrDirPath='/projects/earthtime/air-data/hrrr'):
    # TODO: Change to only return
    # Only used for visualization (currently)
    # Use threading to produce collection of DispersionRuns over several hours for the same source
    # TODO: Check if resolutionHrs param is redundant with emitTimeHrs (check old linRegLib method). Not urgent as long as both are always 1
    hysplitStartLocal = runStartLocal - datetime.timedelta(hours=backwardsHrs)
    hysplitRunTimeHrs = totalRunTimeHrs + backwardsHrs

    hours = list(dateutil.rrule.rrule(dateutil.rrule.HOURLY, interval=resolutionHrs, dtstart=hysplitStartLocal, until=runStartLocal + datetime.timedelta(hours=totalRunTimeHrs-1)))

    # TODO: switch to process pool?
    maxThreads = 30
    pool = SimpleThreadPoolExecutor(maxThreads)
    for i,hour in enumerate(hours):
        run = CachedDispersionRun(
            source=source,
            runStartLocal=hour,
            emitTimeHrs=emitTimeHrs,
            runTimeHrs=min(hysplitRunTimeHrs-(i*resolutionHrs),24),
            hysplitModelSettings=hysplitModelSettings,
            dispersionCachePath=dispersionCachePath,
            hrrrDirPath=hrrrDirPath
            )
        pool.submit(run.findOrRun)
    pathList = pool.shutdown()
    return pathList
