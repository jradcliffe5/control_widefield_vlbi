import os, sys, json, ast, inspect
import numpy as np

filename = inspect.getframeinfo(inspect.currentframe()).filename
sys.path.append(os.path.dirname(os.path.realpath(filename)))

import vex
from correlator_functions import *

try:
	i = sys.argv.index("-c") + 2
except:
	i = 1
	pass

inputs = headless(sys.argv[i])
bb_loc = ast.literal_eval(inputs['baseband_location'])
o_dir = ast.literal_eval(inputs["output_dir"])
vexfile = vex.Vex(ast.literal_eval(inputs["vex_file"]))
ctrl_file = {}

### READ INPUT FILE ###
for i in ["exper_name","cross_polarize","number_channels","slices_per_integration","setup_station","integr_time","message_level","slices_per_integration","LO_offset","multi_phase_center","sub_integr_time","fft_size_correlation"]:
    ctrl_file[i] = ast.literal_eval(inputs[i])

if ast.literal_eval(inputs['delay_directory']) == "":
	rmdirs(["%s/%s_delays"%(o_dir,ast.literal_eval(inputs["exper_name"]))])
	os.mkdir("%s/%s_delays"%(o_dir,ast.literal_eval(inputs["exper_name"])))
	ctrl_file["delay_directory"] = "file://%s/%s_delays"%(o_dir,ast.literal_eval(inputs["exper_name"]))
else:
	ctrl_file["delay_directory"] = "file://%s/%s"%(o_dir,ast.literal_eval(inputs["delay_directory"]))
	
if ast.literal_eval(inputs['tsys_file']) == "":
	ctrl_file["tsys_file"] = "file://%s/%s.tsys"%(o_dir,ast.literal_eval(inputs["exper_name"]))
else:
	ctrl_file["tsys_file"] = "file://%s/%s"%(o_dir,ast.literal_eval(inputs["tsys_file"]))

if ast.literal_eval(inputs['output_file']) == "":
	ctrl_file["output_file"] = "file://%s/%s.corr"%(o_dir,ast.literal_eval(inputs["exper_name"]))
else:
	ctrl_file["output_file"] = "file://%s/%s"%(o_dir,ast.literal_eval(inputs["output_file"]))
########################

#### MAKE CHANNELS #####
corr_chans = []
for i in range(ast.literal_eval(inputs['correlator_channels'])):
	corr_chans.append('CH%02d'%(i+1))
ctrl_file["channels"] = corr_chans
########################

ss={}
ss_s={}
for i in os.listdir('%s'%bb_loc):
	spl=i.split('_')
	scan=spl[2].split('.')[0]
	station=spl[1].capitalize()
	if scan in ss:
		ss[scan]= ss[scan] + [station]
		ss_s[scan] = ss_s[scan] + [i]
	else:
		ss[scan]=[station]
		ss_s[scan] = [i]

del_k = []
for i in ss.keys():
	if len(ss[i]) < ast.literal_eval(inputs['min_stations_per_scan']):
		del_k.append(i)
for i in del_k:
	del ss[i]
	del ss_s[i]
	
stations = []
for i in ss.keys():
	stations.append(ss[i])
stations = flatten_extend(stations)
ctrl_file['stations']=np.unique(stations).tolist()

scans = []
for i in list(ss.keys()):
    scans.append(i.capitalize())
ctrl_file['scans']=scans

import datetime
def find_stop(dt,scan_length):
	dateformat = '%Yy%jd%Hh%Mm%Ss'
	dt = datetime.datetime.strptime(dt,dateformat)
	dt = dt + datetime.timedelta(seconds=scan_length)
	return dt.strftime(dateformat)


print(ss)
if ast.literal_eval(inputs['parallelise_scans']) == True:
	for i in ss.keys():
		sub_ctrl = ctrl_file.copy()
		scan_c = i.capitalize()
		sub_ctrl['scans']=[scan_c]
		sub_ctrl['start']=vexfile['SCHED'][scan_c]['start']
		scan_length = int(vexfile['SCHED'][scan_c]["station"][2].split(" sec")[0])
		sub_ctrl['stop']=find_stop(vexfile['SCHED'][scan_c]['start'],scan_length)

else:
	data_sources = {}
	for i in ss.keys():
		for k,j in enumerate(ss[i]):
			if j in data_sources:
				data_sources[j] = data_sources[j]+['file://%s/%s'%(bb_loc,ss_s[i][k])]
				ctrl_file['start']=vexfile['SCHED'][i.capitalize()]['start']
			else:
				data_sources[j] = ['file://%s/%s'%(bb_loc,ss_s[i][k])]
	scan_length = int(vexfile['SCHED'][i.capitalize()]["station"][2].split(" sec")[0])
	ctrl_file['stop']=find_stop(vexfile['SCHED'][i.capitalize()]['start'],scan_length)
	ctrl_file['data_sources'] = data_sources


	rmfiles(["%s.ctrl"%ast.literal_eval(inputs['exper_name'])])
	with open("%s.ctrl"%ast.literal_eval(inputs['exper_name']), "w") as outfile:
		json.dump(ctrl_file, outfile, indent=4)