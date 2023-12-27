#!/usr/bin/env python
#  Copyright (C) 2020 Aard Keimpema (keimpema@jive.eu)
# 
import sys
import subprocess
import argparse
import re
from vex import Vex
from math import floor
from datetime import datetime, timedelta
FINALS_FILE = "usno_finals.erp"
FINALS_URL = "ftp://gdc.cddis.eosdis.nasa.gov/vlbi/gsfc/ancillary/solve_apriori/" + FINALS_FILE

def vextime(t):
    doy = (t - datetime(t.year, 1, 1)).days + 1
    return "{}y{:03d}d{:02d}h{:02d}m{:02d}s".format(t.year, doy, t.hour, t.minute, t.second)

def from_vextime(vtime):
    y, d, h, m, sec = re.split('[ydhms]', vtime)[:-1]
    t = datetime(int(y), 1, 1, int(h),int(m),int(floor(float(sec)))) + timedelta(days=int(d)-1)
    return t

def jd(t):
    a = (14-t.month)/12
    y = t.year + 4800 - a
    m = t.month + 12*a - 3
    jdn = t.day + ((153*m+2)/5) + 365*y + (y/4) - (y/100) + (y/400) - 32045.5
    return jdn

def vsi_mode(bbcs):
    """ Apply heuristics to determine VSI mode (only astro modes) from bbc dict """
    has_high_bbc = False
    has_even_bbc = False

    for bbc in bbcs:
        has_high_bbc = (bbcs[bbc] >= 9) or has_high_bbc
        has_even_bbc = (bbcs[bbc] & 1 == 0) or has_even_bbc
    if len(bbcs) > 8:
        return 'WASTRO'
    if not has_high_bbc:
        return 'ASTRO'
    if has_high_bbc and has_even_bbc:
        return 'ASTRO2'
    return 'ASTRO3'

def create_threads(vex, thread_names):
    threads_block = ["$THREADS;\n"]
    vsi = {}
    vsi['ASTRO'] = [(i, 'U') for i in range(1, 9)] + [(i, 'L') for i in range(1, 9)]
    vsi['ASTRO2'] = [(i, 'U') for i in range(1, 5)] + [(i, 'U') for i in range(9, 13)] + \
                    [(i, 'L') for i in range(1, 5)] + [(i, 'L') for i in range(9, 13)]
    vsi['ASTRO3'] = [(i, 'U') for i in range(1, 16, 2)] + [(i, 'L') for i in range(1, 16, 2)]
    vsi['WASTRO'] = [(i, 'U') for i in range(1, 9)] + [(i, 'L') for i in range(1, 9)] + \
                    [(i, 'U') for i in range(9, 17)] + [(i, 'L') for i in range(9, 17)]

    for freq_name, bbc_name, mode in thread_names:
        name = thread_names[(freq_name, bbc_name, mode)]
        bbcs = {}
        for line in vex['BBC'][bbc_name].getall('BBC_assign'):
            bbcs[line[0]] = int(line[1])
        vmode = vsi_mode(bbcs)
        bbc_in_freq = []
        for line in vex['FREQ'][freq_name].getall('chan_def'):
            bbc_nr = bbcs[line[5]]
            bbc_in_freq.append((bbc_nr, line[2]))
        bw = float(line[3].split()[0])
        sorted_bbc = []
        for bbc in vsi[vmode]:
            if bbc in bbc_in_freq:
                sorted_bbc.append(bbc)
        threads_block.append('def {};\n'.format(name))
        nchan = len(bbc_in_freq)
        rate = int(nchan * bw * 4) # Assuming nyquist sampling and 2-bit quantization
        threads_block.append('    format = vdif :   : {};\n'.format(rate))
        threads_block.append('    thread = 0 : 1 : 1 : {} : {} : 2 :   :   : 8000;\n'.format(rate, nchan))
        for i, line in enumerate(vex['FREQ'][freq_name].getall('chan_def')):
            index = sorted_bbc.index(bbc_in_freq[i])
            label = line[4]
            threads_block.append("    channel = {} : 0 : {};\n".format(label, index))
        threads_block.append("enddef;\n")
    return threads_block

def create_tapelogobs(stations, start, stop):
    s1 = vextime(start)
    s2 = vextime(stop)
    lines = ["$TAPELOG_OBS;\n"]
    for station in stations:
        lines.append("def {};\n".format(station))
        lines.append("    VSN = 1 : eVLBI : {} : {} ;\n".format(s1, s2))
        lines.append("enddef;\n")
    return lines

def create_clocks(stations, start, stop):
    tmid = start + (stop - start) / 2
    vexstart = vextime(start)
    vexmid = vextime(tmid)
    lines = ["$CLOCK;\n"]
    for station in stations:
        lines.append("def {};\n".format(station))
        lines.append("    clock_early = {} : 0.0000 usec : {} : 0.0000 usec/sec;\n".format(vexstart, vexmid))
        lines.append("enddef;\n")
    return lines

def create_eop(exp_start, exp_stop):
    eop_start = jd(exp_start - timedelta(days=1))
    eop_stop = jd(exp_stop + timedelta(days=1))
    if eop_start >= 2457754.5: # 2017-01-01
        taiutc = 37
    elif eop_start >= 2457203.5: # 2015-30-06
        taiutc = 36
    elif eop_start >= 2456109.5: # 2012-30-06
        taiutc = 35
    else:
        taiutc = 34

    # First try reading an existing finals file
    try:
        fp = open(FINALS_FILE, 'r')
    except IOError:
        download_eop()
        fp = open(FINALS_FILE, 'r')
    # Check if file is new enough, note that we can't just look at the
    # last eop date in the data, because all values after a certain
    # date will be based on prediction rather than measurement
    lines = fp.readlines()
    readnew = True
    for line in lines[1:]:
        # If last good entry was within two days then we don't need to re-download
        if line.find("date with real data:") != -1:
            y,m,d = [int(d) for d in line.split()[-1].split('.')]
            lastgood = datetime(y, m, d)
            if (exp_stop <= lastgood) or (datetime.now() - lastgood).days <= 2:
                readnew = False
                break
    if readnew:
        download_eop()
    fp = open(FINALS_FILE, 'r')
    lines = fp.readlines()

    # Now that we have the most recent EOP files, parse the tables
    x_wobble = []
    y_wobble = []
    ut1utc = []
    for line in lines[1:]:
        if not line.startswith("#"):
            t = float(line.split()[0])
            if  floor(t) > floor(eop_stop):
                break
            if  floor(t) >= floor(eop_start):
                data = [float(f) for f in line.split()]
                x_wobble.append("{:.6f}".format(data[1] / 10.))
                y_wobble.append("{:.6f}".format(data[2] / 10.))
                ut1utc.append("{:.7f}".format(data[3] / 1e6 + taiutc))

    eop = "$EOP;\ndef theEOP;\n"
    eop += "    TAI-UTC = {} sec;\n".format(taiutc)
    yday = (exp_start - datetime(exp_start.year, 1, 1)).days # eop starts 1 day before exp_start
    eop += "    eop_ref_epoch = {}y{:03d}d00h00m00s;\n".format(exp_start.year, yday)
    eop += "    eop_interval = 24 hr;\n"
    eop += "    num_eop_points = {};\n".format(len(ut1utc))
    eop += "    x_wobble = {} asec;\n".format(" asec : ".join(x_wobble))
    eop += "    y_wobble = {} asec;\n".format(" asec : ".join(y_wobble))
    eop += "    ut1-utc = {} sec;\n".format(" sec : ".join(ut1utc))
    eop += "enddef;\n"
    return eop

def download_eop():
    cmd = 'curl -O --ftp-ssl {0}'.format(FINALS_URL)
    subprocess.call(cmd, shell=True)

def get_stations(vex):
    """ Make list of stations in vex file, only return stations which have observed """
    stations = set()
    for scan in vex['SCHED']:
        for station in vex['SCHED'][scan].getall('station'):
            stations.add(station[0])
    return sorted(list(stations))

def get_experiment_timerange(vex):
    """ Determine experiment start and stop times """
    first = True
    for scan in vex['SCHED']:
        if first:
            start = from_vextime(vex['SCHED'][scan]['start'])
            nsec = int(vex['SCHED'][scan]['station'][2].split()[0])
            stop = start + timedelta(seconds=nsec)
            first = False
        curstart = from_vextime(vex['SCHED'][scan]['start'])
        nsec = 0
        for station in vex['SCHED'][scan].getall('station'):
            cur_nsec = int(station[2].split()[0])
            nsec = max(nsec, cur_nsec)
        curstop = curstart + timedelta(seconds=nsec)
        start = min(start, curstart)
        stop = max(stop, curstop)
    return start, stop

def get_threads_map(vex):
    thread_names = {}
    threads_map = {}
    for mode in vex['MODE']:
        for freq in vex['MODE'][mode].getall('FREQ'):
            for station in freq[1:]:
                for bbc in vex['MODE'][mode].getall('BBC'):
                    if station in bbc:
                        key = (freq[0], bbc[0], mode)
                        try:
                            threads_map[key].append(station)
                        except KeyError:
                            threads_map[key] = [station]
                            thread_names[key] = "THREADS.{}.{}".format(mode, station)
    return thread_names, threads_map

#########
########################## MAIN #################################3
########
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Update vex file for use with SFXC')
    parser.add_argument('invex', help='Input vex file')
    parser.add_argument('outvex', help='Output vex file')
    args = parser.parse_args()
    vex = Vex(args.invex)
    invex = file(args.invex, 'r')
    outvex = file(args.outvex, 'w')
    stations = get_stations(vex)
    start, stop = get_experiment_timerange(vex)
    threads_names, threads_map = get_threads_map(vex)
    vex_section = ['$ROOT', '$GLOBAL', '$EXPER', '$MODE', '$STATION', '$ANTENNA', \
                   '$SITE', '$IF', '$BBC', '$FREQ', '$TRACKS', '$THREADS', '$DAS', \
                   '$SOURCE', '$TAPELOG_OBS', '$SCHED', '$CLOCK', '$EOP']
    section = '$ROOT'
    line = invex.readline()
    textvex = {section: []}
    while line != "":
        sline = line.lstrip()
        if sline.startswith('$'):
            section = sline.split(';')[0]
            textvex[section] = []
        textvex[section].append(line)
        if section == '$GLOBAL' and sline.startswith('$'):
            textvex[section].append('    ref $EOP = theEOP;\n')
        elif section == "$MODE":
            if sline.startswith('def'):
                mode = sline.split()[1].split(';')[0]
                for key in threads_names:
                    if key[2] == mode:
                        textvex[section].append('    ref $THREADS = {}:{};\n'.format(threads_names[key], ':'.join(threads_map[key])))
        elif section == "$STATION":
            if sline.startswith('def'):
                st = sline.split()[1].split(';')[0]
                textvex[section].append('    ref $TAPELOG_OBS = {};\n'.format(st))
                textvex[section].append('    ref $CLOCK = {};\n'.format(st))
        line = invex.readline()
    textvex['$THREADS'] = create_threads(vex, threads_names)
    textvex['$TAPELOG_OBS'] = create_tapelogobs(stations, start, stop)
    textvex['$CLOCK'] = create_clocks(stations, start, stop)
    textvex['$EOP'] = create_eop(start, stop)
    for section in vex_section:
        for line in textvex[section]:
            outvex.write(line)
