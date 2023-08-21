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
sfxc_exec = ast.literal_eval(inputs['sfxc_exec'])
ctrl_file = {}

### READ INPUT FILE ###
for i in ["exper_name","cross_polarize","number_channels","slices_per_integration","setup_station","integr_time","message_level","slices_per_integration","LO_offset","multi_phase_center","sub_integr_time","fft_size_correlation"]:
    ctrl_file[i] = ast.literal_eval(inputs[i])
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

commands = []
corr_files = {}
if ast.literal_eval(inputs['parallelise_scans']) == True:
	rmdirs(["%s/%s_delays"%(o_dir,ctrl_file["exper_name"])])
	os.mkdir("%s/%s_delays"%(o_dir,ctrl_file["exper_name"]))
	for i in ss.keys():
		scan_c = i.capitalize()
		sub_ctrl = ctrl_file.copy()
		rmdirs(["%s/%s"%(o_dir,scan_c)])
		os.mkdir("%s/%s"%(o_dir,scan_c))
		sub_ctrl["delay_directory"] = "file://%s/%s_delays"%(o_dir,ctrl_file["exper_name"])
		sub_ctrl["tsys_file"] = "file://%s/%s/%s.tsys"%(o_dir,scan_c,ctrl_file["exper_name"])
		sub_ctrl['output_file'] = "file://%s/%s/%s.%s.cor"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c)
		sub_ctrl['scans']=[scan_c]
		sub_ctrl['start']=vexfile['SCHED'][scan_c]['start']
		scan_length = int(vexfile['SCHED'][scan_c]["station"][2].split(" sec")[0])
		sub_ctrl['stop']=find_stop(vexfile['SCHED'][scan_c]['start'],scan_length)
		sub_ctrl['stations'] = ss[i]
		if ctrl_file['multi_phase_center'] == 'auto':
			if len(vexfile['SCHED'][scan_c]['source']) > 1:
				sub_ctrl['multi_phase_center'] = True
			else:
				sub_ctrl['multi_phase_center'] = False
		else:
			sub_ctrl['multi_phase_center'] = ctrl_file['multi_phase_center']
		data_sources = {}
		for k,j in enumerate(ss[i]):
			if j in data_sources:
				data_sources[j] = data_sources[j]+['file://%s/%s'%(bb_loc,ss_s[i][k])]
			else:
				data_sources[j] = ['file://%s/%s'%(bb_loc,ss_s[i][k])]
		sub_ctrl['data_sources'] = data_sources
		rmfiles(["%s/%s/%s.%s.ctrl"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c)])
		with open("%s/%s/%s.%s.ctrl"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c), "w") as outfile:
			json.dump(sub_ctrl, outfile, indent=4)
		commands.append('%s %s/%s/%s.%s.ctrl %s'%(sfxc_exec,o_dir,scan_c,ctrl_file["exper_name"],scan_c,ast.literal_eval(inputs["vex_file"])))
		for j in vexfile['SCHED'][scan_c]['source']:
			if len(vexfile['SCHED'][scan_c]['source']) == 1:
				if ctrl_file["exper_name"] in list(corr_files.keys()):
					corr_files[ctrl_file["exper_name"]] = corr_files[ctrl_file["exper_name"]].append("%s/%s/%s.%s.cor"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c))
				else:
					corr_files[ctrl_file["exper_name"]] = ["%s/%s/%s.%s.cor"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c)]
			elif j == inputs["calibrator_target"]:
				if ctrl_file["exper_name"] in list(corr_files.keys()):
					corr_files[ctrl_file["exper_name"]] = corr_files[ctrl_file["exper_name"]].append("%s/%s/%s.%s.cor_%s"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c,j))
				else:
					corr_files[ctrl_file["exper_name"]] = ["%s/%s/%s.%s.cor_%s"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c,j)]
			else:
				if j in list(corr_files.keys()):
					corr_files[j] = corr_files[j].append("%s/%s/%s.%s.cor_%s"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c,j))
				else:
					corr_files[j] = ["%s/%s/%s.%s.cor_%s"%(o_dir,scan_c,ctrl_file["exper_name"],scan_c,j)]
	write_job(step='run_sfxc',commands=commands,job_manager='bash')
	commands = []
	for i in list(corr_files.keys()):
		commands.append('%s %s -o %s.ms'%(ast.literal_eval(inputs["j2ms2_exec"])," ".join(corr_files[i]),i))
	write_job(step='run_j2ms2',commands=commands,job_manager='bash')

				

else:
	data_sources = {}
	if ast.literal_eval(inputs['delay_directory']) == "":
		rmdirs(["%s/%s_delays"%(o_dir,ctrl_file["exper_name"])])
		os.mkdir("%s/%s_delays"%(o_dir,ctrl_file["exper_name"]))
		ctrl_file["delay_directory"] = "file://%s/%s_delays"%(o_dir,ctrl_file["exper_name"])
	else:
		ctrl_file["delay_directory"] = "file://%s/%s"%(o_dir,ctrl_file["exper_name"])
	if ast.literal_eval(inputs['tsys_file']) == "":
		ctrl_file["tsys_file"] = "file://%s/%s.tsys"%(o_dir,ctrl_file["exper_name"])
	else:
		ctrl_file["tsys_file"] = "file://%s/%s"%(o_dir,ctrl_file["exper_name"])
	if ast.literal_eval(inputs['output_file']) == "":
		ctrl_file["output_file"] = "file://%s/%s.cor"%(o_dir,ast.literal_eval(inputs["exper_name"]))
	else:
		ctrl_file["output_file"] = "file://%s/%s"%(o_dir,ast.literal_eval(inputs["output_file"]))
	for i in ss.keys():
		if ctrl_file['multi_phase_center'] == "auto":
			if len(vexfile['SCHED'][i.capitalize()]['source']) > 1:
				ctrl_file['multi_phase_center'] = True
			else:
				pass
		for k,j in enumerate(ss[i]):
			if j in data_sources:
				data_sources[j] = data_sources[j]+['file://%s/%s'%(bb_loc,ss_s[i][k])]
			else:
				data_sources[j] = ['file://%s/%s'%(bb_loc,ss_s[i][k])]
				ctrl_file['start']=vexfile['SCHED'][i.capitalize()]['start']
	if ctrl_file['multi_phase_center'] == "auto":
		ctrl_file['multi_phase_center'] = False
	scan_length = int(vexfile['SCHED'][i.capitalize()]["station"][2].split(" sec")[0])
	ctrl_file['stop']=find_stop(vexfile['SCHED'][i.capitalize()]['start'],scan_length)
	ctrl_file['data_sources'] = data_sources

	rmfiles(["%s.ctrl"%ast.literal_eval(inputs['exper_name'])])
	with open("%s.ctrl"%ast.literal_eval(inputs['exper_name']), "w") as outfile:
		json.dump(ctrl_file, outfile, indent=4)