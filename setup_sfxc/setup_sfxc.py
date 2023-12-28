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
produce_html_plot_exec = ast.literal_eval(inputs['produce_html_plot_exec'])
recorrelate = ast.literal_eval(inputs['recorrelate_targets'])
ctrl_file = {}

### READ INPUT FILE ###
for i in ["exper_name","cross_polarize","number_channels","normalize","slices_per_integration","setup_station","integr_time","message_level","slices_per_integration","LO_offset","multi_phase_center","sub_integr_time","fft_size_correlation"]:
    ctrl_file[i] = ast.literal_eval(inputs[i])
########################

#### MAKE CHANNELS #####
corr_chans = []
for i in range(ast.literal_eval(inputs['correlator_channels'])):
	corr_chans.append('CH%02d'%(i+1))
ctrl_file["channels"] = corr_chans
########################

ss={}
### NEW VIX FILE READING
for i in vexfile['SCHED'].keys():
	#print(i)
	scan=i
	station = []
	for j in vexfile['SCHED'][i]['station']:
		station.append(j[0])
	ss[scan] = station


ss_s = {}
data_s = {}
for i in ss.keys(): ## i is scans
	ds = []
	for j in ss[i]:
		if os.path.exists('%s/%s_%s_%s.m5a'%(bb_loc,ctrl_file['exper_name'].lower(),j.lower(),i.lower())):
			ds.append('%s_%s_%s.m5a'%(ctrl_file['exper_name'].lower(),j.lower(),i.lower()))
			data_s[j] = '%s_%s_%s.m5a'%(ctrl_file['exper_name'].lower(),j.lower(),i.lower())
		else:
			#print(j)
			ds.append(data_s[j])
	ss_s[i] = ds


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
ss_temp = {}
for i in list(ss.keys()):
	if ast.literal_eval(inputs['do_clock_search']) == True:
		if i.capitalize() in ast.literal_eval(inputs['fringe_finder_scans']):
			scans.append(i.capitalize())
			ss_temp[i] = ss[i]
	else:
		scans.append(i.capitalize())
ctrl_file['scans']=scans

if os.path.exists("%s/logs"%(o_dir)) == False:
	os.mkdir("%s/logs"%(o_dir))

if ast.literal_eval(inputs['do_clock_search']) == True:
	ss = ss_temp
	cs = "clock_search/"
	rmdirs(["%s/%s"%(o_dir,cs)])
	os.mkdir("%s/%s"%(o_dir,cs))
	ctrl_file["number_channels"] = ast.literal_eval(inputs['clock_nchannels'])
else:
	cs = "correlation/"
	if recorrelate == True:
		rc = 1
	else:
		rc = 0
		rmdirs(["%s/%s"%(o_dir,cs)])
		os.mkdir("%s/%s"%(o_dir,cs))

def remote_mkdir(dir="",remote=False,commands=[]):
	if remote==True:
		commands.append('mkdir %s'%dir)
	else:
		if os.path.exists("%s"%dir):
			pass
		else:
			os.mkdir("%s"%dir)
	return commands

def split_scans(scan_dict,ratios=[],names=[]):
	keys = np.array(list(scan_dict.keys()))
	assert np.sum(ratios)==1.0
	size = len(keys)
	idx = []
	ct = 0
	for j,i in enumerate(ratios):
		if j != (len(ratios)-1):
			idx.append(ct+int(size*i))
			ct = int(size*i)
	print(idx)
	spl = np.array_split(keys,idx)
	newdict = {}
	for j,key in enumerate(spl):
		newdict[names[j]] = {}
		for i in key:
			newdict[names[j]][i] = scan_dict[i]
	return newdict

def generate_correlator_environment(exper="",vexfile={},scans={},datasources={},c_name="",inputs={}):
	"""
	Function aims to generate the environment for the ctrl files
	"""
	if c_name == 'localhost':
		remote=False
	else:
		remote=True
	commands = []
	corr_files = {}
	if ast.literal_eval(inputs['parallelise_scans']) == True:
		commands = remote_mkdir(dir="%s/%s%s_delays"%(o_dir,cs,exper),remote=remote,commands=commands)
		for i in scans.keys():
			scan_c = i.capitalize()
			if len(vexfile['SCHED'][scan_c]['source']) > rc:
				sub_ctrl = ctrl_file.copy()
				#rmdirs(["%s/%s%s"%(o_dir,cs,scan_c)])
				if len(vexfile['SCHED'][scan_c]['source']) > 10:
					srcs = ", ".join(vexfile['SCHED'][scan_c]['source'][0:10]) + ' + ... (total: %d sources)'%len(vexfile['SCHED'][scan_c]['source'])
				else:
					srcs = ", ".join(vexfile['SCHED'][scan_c]['source'])
				print('Making the following correlator scans: %s for sources: %s'%(scan_c,srcs))
				if os.path.exists("%s/%s%s"%(o_dir,cs,scan_c)):
					pass
				else:
					os.mkdir("%s/%s%s"%(o_dir,cs,scan_c))
				sub_ctrl["delay_directory"] = "file://%s/%s%s_delays"%(o_dir,cs,exper)
				sub_ctrl["tsys_file"] = "file://%s/%s%s/%s.tsys"%(o_dir,cs,scan_c,exper)
				sub_ctrl['output_file'] = "file://%s/%s%s/%s.%s.cor"%(o_dir,cs,scan_c,exper,scan_c)
				sub_ctrl['scans']=[scan_c]
				sub_ctrl['start']=vexfile['SCHED'][scan_c]['start']
				if ast.literal_eval(inputs['do_clock_search']) == True:
					os.mkdir("%s/%s%s/plots"%(o_dir,cs,scan_c))
					sub_ctrl['start']=find_stop(vexfile['SCHED'][scan_c]['start'],ast.literal_eval(inputs['begin_delay']))
					sub_ctrl['stop']=find_stop(sub_ctrl['start'],ast.literal_eval(inputs['time_on']))
				else:
					scan_length = int(vexfile['SCHED'][scan_c]["station"][0][2].split(" sec")[0])
					sub_ctrl['stop']=find_stop(vexfile['SCHED'][scan_c]['start'],scan_length)
				sub_ctrl['stations'] = scans[i]
				if ctrl_file['multi_phase_center'] == 'auto':
					if len(vexfile['SCHED'][scan_c]['source']) > 1:
						sub_ctrl['multi_phase_center'] = True
					else:
						sub_ctrl['multi_phase_center'] = False
				else:
					sub_ctrl['multi_phase_center'] = ctrl_file['multi_phase_center']
				data_sources = {}
				for k,j in enumerate(scans[i]):
					if j in data_sources:
						data_sources[j] = data_sources[j]+['file://%s/%s'%(bb_loc,datasources[i][k])]
					else:
						data_sources[j] = ['file://%s/%s'%(bb_loc,datasources[i][k])]
				sub_ctrl['data_sources'] = data_sources
				#rmfiles(["%s/%s%s/%s.%s.ctrl"%(o_dir,cs,scan_c,exper,scan_c)])
				with open("%s/%s%s/%s.%s.ctrl"%(o_dir,cs,scan_c,exper,scan_c), "w") as outfile:
					json.dump(sub_ctrl, outfile, indent=4)
				commands.append('%s %s/%s%s/%s.%s.ctrl %s 2>&1 | tee %s/logs/sfxc_run.log'%(sfxc_exec,o_dir,cs,scan_c,exper,scan_c,ast.literal_eval(inputs["vex_file"]),o_dir))
				if ast.literal_eval(inputs['do_clock_search']) == True:
					commands.append('%s %s %s/%s%s/%s.%s.cor %s/%s%s/plots'%(produce_html_plot_exec,ast.literal_eval(inputs["vex_file"]),o_dir,cs,scan_c,exper,scan_c,o_dir,cs,scan_c))
			for j in vexfile['SCHED'][scan_c]['source']:
				if j == ast.literal_eval(inputs["calibrator_target"]):
					if exper in list(corr_files.keys()):
						corr_files[exper] = corr_files[exper]+["%s%s/%s.%s.cor_%s"%(cs,scan_c,exper,scan_c,j)]
					else:
						corr_files[exper] = ["%s%s/%s.%s.cor_%s"%(cs,scan_c,exper,scan_c,j)]
				elif len(vexfile['SCHED'][scan_c]['source']) == 1:
					if exper in list(corr_files.keys()):
						corr_files[exper] = corr_files[exper] + ["%s%s/%s.%s.cor"%(cs,scan_c,exper,scan_c)]
					else:
						corr_files[exper] = ["%s%s/%s.%s.cor"%(cs,scan_c,exper,scan_c)]
				else:
					if j in list(corr_files.keys()):
						corr_files[j] = corr_files[j] + ["%s%s/%s.%s.cor_%s"%(cs,scan_c,exper,scan_c,j)]
					else:
						corr_files[j] = ["%s%s/%s.%s.cor_%s"%(cs,scan_c,exper,scan_c,j)]
			else:
				pass
		if ast.literal_eval(inputs['do_clock_search']) == True:
			write_job(step='run_clocksearch_sfxc',commands=commands,job_manager='bash',write='w')
		else:
			commands.append('rm chex.*')
			write_job(step='run_sfxc',commands=commands,job_manager='bash',write='w')
			commands = []
			print('Building script for conversion to measurement sets')
			ms_output = ast.literal_eval(inputs['ms_output'])
			for i in list(corr_files.keys()):
				print('Source %s .. done'%i)
				if ms_output =="":
					commands.append('%s %s -o %s/%s.ms 2>&1 | tee %s/logs/j2ms2_%s.log &'%(ast.literal_eval(inputs["j2ms2_exec"])," ".join(corr_files[i]),o_dir,i,o_dir,i))
				else:
					commands.append('%s %s -o %s/%s.ms 2>&1 | tee %s/logs/j2ms2_%s.log &'%(ast.literal_eval(inputs["j2ms2_exec"])," ".join(corr_files[i]),ms_output,i,o_dir,i))
			commands[-1] = commands[-1].split(' &')[0]
			if ms_output !="":
				for i in list(corr_files.keys()):
						commands.append('cp -rv %s/%s.ms %s &'%(ms_output,i,o_dir))
				commands[-1] = commands[-1].split(' &')[0]
			write_job(step='run_j2ms2',commands=commands,job_manager='bash',write='w')
			print('Building script for flagging of low correlator weights')
			commands = ['parallel -eta -j 40 %s sfxc_helperscripts/post_processing/flag_weights.py {} %.3f ::: *.ms 2>&1 | tee %s/logs/flag_weights.log'%(inputs['casa_exec'],ast.literal_eval(inputs['flag_threshold']),o_dir)]
			write_job(step='run_flag_data',commands=commands,job_manager='bash',write='w')


	else:
		data_sources = {}
		if ast.literal_eval(inputs['delay_directory']) == "":
			rmdirs(["%s/%s%s_delays"%(o_dir,cs,exper)])
			os.mkdir("%s/%s%s_delays"%(o_dir,cs,exper))
			ctrl_file["delay_directory"] = "file://%s/%s%s_delays"%(o_dir,cs,exper)
		else:
			ctrl_file["delay_directory"] = "file://%s/%s%s"%(o_dir,cs,exper)
		if ast.literal_eval(inputs['tsys_file']) == "":
			ctrl_file["tsys_file"] = "file://%s/%s%s.tsys"%(o_dir,cs,exper)
		else:
			ctrl_file["tsys_file"] = "file://%s/%s%s"%(o_dir,cs,exper)
		if ast.literal_eval(inputs['output_file']) == "":
			ctrl_file["output_file"] = "file://%s/%s%s.cor"%(o_dir,cs,ast.literal_eval(inputs["exper_name"]))
		else:
			ctrl_file["output_file"] = "file://%s/%s%s"%(o_dir,cs,ast.literal_eval(inputs["output_file"]))
		for i in scans.keys():
			if ctrl_file['multi_phase_center'] == "auto":
				if len(vexfile['SCHED'][i.capitalize()]['source']) > 1:
					ctrl_file['multi_phase_center'] = True
				else:
					pass
			for k,j in enumerate(scans[i]):
				if j in data_sources:
					data_sources[j] = data_sources[j]+['file://%s/%s'%(bb_loc,datasources[i][k])]
				else:
					data_sources[j] = ['file://%s/%s'%(bb_loc,datasources[i][k])]
					ctrl_file['start']=vexfile['SCHED'][i.capitalize()]['start']
		if ctrl_file['multi_phase_center'] == "auto":
			ctrl_file['multi_phase_center'] = False
		scan_length = int(vexfile['SCHED'][i.capitalize()]["station"][0][2].split(" sec")[0])
		ctrl_file['stop']=find_stop(vexfile['SCHED'][i.capitalize()]['start'],scan_length)
		ctrl_file['data_sources'] = data_sources

		rmfiles(["%s/%s%s.ctrl"%(o_dir,cs,ast.literal_eval(inputs['exper_name']))])
		with open("%s/%s%s.ctrl"%(o_dir,cs,ast.literal_eval(inputs['exper_name'])), "w") as outfile:
			json.dump(ctrl_file, outfile, indent=4)
