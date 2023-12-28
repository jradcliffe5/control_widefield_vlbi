import os, glob, re, datetime, ast
import numpy as np
import json, collections
from collections import OrderedDict

class NpEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, np.integer):
			return int(obj)
		elif isinstance(obj, np.floating):
			return float(obj)
		elif isinstance(obj, np.ndarray):
			return obj.tolist()
		else:
			return super(NpEncoder, self).default(obj)

def json_load_byteified(file_handle):
	return _byteify(
		json.load(file_handle, object_hook=_byteify),
		ignore_dicts=True
	)

def json_loads_byteified(json_text):
	return _byteify(
		json.loads(json_text, object_hook=_byteify),
		ignore_dicts=True
	)

def json_load_byteified_dict(file_handle,casa6):
	if casa6==True:
		return convert_temp(_byteify(
			json.load(file_handle, object_hook=_byteify, object_pairs_hook=OrderedDict),
			ignore_dicts=True))
	else:
		return convert(_byteify(
			json.load(file_handle, object_hook=_byteify, object_pairs_hook=OrderedDict),
			ignore_dicts=True))

def json_loads_byteified_dict(json_text,casa6):
	if casa6==True:
		return convert_temp(_byteify(
			json.loads(json_text, object_hook=_byteify, object_pairs_hook=OrderedDict),
			ignore_dicts=True))
	else:
		return convert(_byteify(
			json.loads(json_text, object_hook=_byteify, object_pairs_hook=OrderedDict),
			ignore_dicts=True))

def convert(data):
	if isinstance(data, basestring):
		return str(data)
	elif isinstance(data, collections.Mapping):
		return OrderedDict(map(convert, data.iteritems()))
	elif isinstance(data, collections.Iterable):
		return type(data)(map(convert, data))
	else:
		return data

def convert_temp(data):
	if isinstance(data, str):
		return str(data)
	elif isinstance(data, collections.Mapping):
		return OrderedDict(map(convert_temp, data.items()))
	elif isinstance(data, collections.Iterable):
		return type(data)(map(convert_temp, data))
	else:
		return data

def _byteify(data, ignore_dicts=False):
	# if this is a unicode string, return its string representation
	try:
		if isinstance(data, unicode):
			return data.encode('utf-8')
	except: 
		if isinstance(data, str):
			return data
	# if this is a list of values, return list of byteified values
	if isinstance(data, list):
		return [ _byteify(item, ignore_dicts=True) for item in data ]
	# if this is a dictionary, return dictionary of byteified keys and values
	# but only if we haven't already byteified it
	if isinstance(data, dict) and not ignore_dicts:
		try:
			return {
				_byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
				for key, value in data.iteritems()
			}
		except:
			return {
				_byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
				for key, value in data.items()
			}
	# if it's anything else, return it in its original form
	return data

def load_json(filename,Odict=False,casa6=False):
	if Odict==False:
		with open(filename, "r") as f:
			json_data = json_load_byteified(f)
		f.close()
	else:
		with open(filename, "r") as f:
			json_data = json_load_byteified_dict(f,casa6)
		f.close()
	return json_data

def save_json(filename,array,append=False):
	if append==False:
		write_mode='w'
	else:
		write_mode='a'
	with open(filename, write_mode) as f:
		json.dump(array, f,indent=4, separators=(',', ': '),cls=NpEncoder)
	f.close()

def flatten_extend(matrix):
    flat_list = []
    for row in matrix:
        flat_list.extend(row)
    return flat_list

def write_job(step,commands,job_manager,write):
	with open('./job_%s.%s'%(step,job_manager), write) as filehandle:
			for listitem in commands:
				filehandle.write('%s\n' % listitem)

def rmfiles(files):
	for i in files:
		if "*" in i:
			files_to_die = glob.glob(i)
			print('Files matching with %s - deleting'% i)
			for j in files_to_die:
				if os.path.exists(j) == True:
					print('File %s found - deleting'% j)
					os.system('rm %s'%j)
				else:
					pass
		elif os.path.exists(i) == True:
			print('File %s found - deleting'% i)
			os.system('rm %s'%i)
		else:
			print('No file found - %s'% i)
	return

def rmdirs(dirs):
	for i in dirs:
		if "*" in i:
			files_to_die = glob.glob(i)
			print('Directories matching with %s - deleting'% i)
			for j in files_to_die:
				if os.path.exists(j) == True:
					print('Directory/table %s found - deleting'% j)
					os.system('rm -r %s'%j)
				else:
					pass
		elif os.path.exists(i) == True:
			print('Directory/table %s found - deleting'% i)
			os.system('rm -r %s'%i)
		else:
			print('No file found - %s'% i)
	return

def headless(inputfile):
	''' Parse the list of inputs given in the specified file. (Modified from evn_funcs.py)'''
	INPUTFILE = open(inputfile, "r")
	control = {}
	# a few useful regular expressions
	newline = re.compile(r'\n')
	space = re.compile(r'\s')
	char = re.compile(r'\w')
	comment = re.compile(r'#.*')
	# parse the input file assuming '=' is used to separate names from values
	for line in INPUTFILE:
		if char.match(line):
			line = comment.sub(r'', line)
			line = line.replace("'", '')
			(param, value) = line.split('=')
			param = newline.sub(r'', param)
			param = param.strip()
			param = space.sub(r'', param)
			value = newline.sub(r'', value)
			value = value.strip()
			valuelist = value.split(',')
			if len(valuelist) == 1:
				control[param] = str(valuelist[0])
			else:
				control[param] = ','.join(valuelist)
	return control

def find_stop(dt,scan_length):
	dateformat = '%Yy%jd%Hh%Mm%Ss'
	dt = datetime.datetime.strptime(dt,dateformat)
	dt = dt + datetime.timedelta(seconds=scan_length)
	return dt.strftime(dateformat)

def build_master_ctrl_file(inputs,vexfile):
	ctrl_file = {}
	bb_loc = inputs['baseband_location']
	### READ INPUT FILE ###
	for i in ["exper_name","cross_polarize","number_channels","normalize","slices_per_integration","setup_station","integr_time","message_level","slices_per_integration","LO_offset","multi_phase_center","sub_integr_time","fft_size_correlation"]:
		ctrl_file[i] = inputs[i]
	########################

	#### MAKE CHANNELS #####
	corr_chans = []
	for i in range(inputs['correlator_channels']):
		corr_chans.append('CH%02d'%(i+1))
	ctrl_file["channels"] = corr_chans
	########################

	#### GET STATIONS PER SCAN #####
	ss={}
	for i in vexfile['SCHED'].keys():
		scan=i
		station = []
		for j in vexfile['SCHED'][i]['station']:
			station.append(j[0])
		ss[scan] = station
	################################

	#### GET BASEBAND DATA PER SCAN #####
	ss_s = {}
	data_s = {}
	for i in ss.keys(): 
		ds = []
		for j in ss[i]:
			if os.path.exists('%s/%s_%s_%s.m5a'%(bb_loc,ctrl_file['exper_name'].lower(),j.lower(),i.lower())):
				ds.append('%s_%s_%s.m5a'%(ctrl_file['exper_name'].lower(),j.lower(),i.lower()))
				data_s[j] = '%s_%s_%s.m5a'%(ctrl_file['exper_name'].lower(),j.lower(),i.lower())
			else:
				ds.append(data_s[j]) ## Uses last datasource if data not found 
		ss_s[i] = ds


	del_k = []
	for i in ss.keys():
		if len(ss[i]) < inputs['min_stations_per_scan']:
			del_k.append(i) ## Remove scans with less than min_stations in input
	for i in del_k:
		del ss[i]
		del ss_s[i]
		
	#### GET UNIQUE STATIONS PER SCAN (IF NOT PARALLELISING) #####
	stations = []
	for i in ss.keys():
		stations.append(ss[i])
	stations = flatten_extend(stations)
	ctrl_file['stations']=np.unique(stations).tolist()


	#### SELECT SCANS IF DOING CLOCK SEARCHES #####
	scans = []
	ss_temp = {}
	for i in list(ss.keys()):
		if inputs['do_clock_search'] == True:
			if i.capitalize() in inputs['fringe_finder_scans']:
				scans.append(i.capitalize())
				ss_temp[i] = ss[i]
		else:
			scans.append(i.capitalize())
	ctrl_file['scans']=scans
	if inputs['do_clock_search'] == True:
		ss = ss_temp
		ctrl_file["number_channels"] = inputs['clock_nchannels']
	return ctrl_file, ss, ss_s

def build_directory_structure(o_dir="",recorrelate=False,clocksearch=False,scans={}):
	if os.path.exists("%s/logs"%(o_dir)) == False:
		os.mkdir("%s/logs"%(o_dir))

	if clocksearch == True:
		cs = "clock_search/"
		rmdirs(["%s/%s"%(o_dir,cs)])
		os.mkdir("%s/%s"%(o_dir,cs))
	else:
		cs = "correlation/"
		if recorrelate == False:
			rmdirs(["%s/%s"%(o_dir,cs)])
			os.mkdir("%s/%s"%(o_dir,cs))

	for i in scans.keys():
		scan_c = i.capitalize()
		if os.path.exists("%s/%s%s"%(o_dir,cs,scan_c)):
			pass
		else:
			os.mkdir("%s/%s%s"%(o_dir,cs,scan_c))
	return

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

def generate_correlator_environment(exper="",vexfile={},scans={},datasources={},c_name="",inputs={},ctrl_file={}):
	"""
	Function aims to generate the environment for the ctrl files
	"""
	o_dir = inputs['output_dir']
	exper = inputs['exper_name']
	bb_loc = inputs['baseband_location']
	o_dir = inputs["output_dir"]
	sfxc_exec = inputs['sfxc_exec']
	produce_html_plot_exec = inputs['produce_html_plot_exec']
	recorrelate = inputs['recorrelate_targets']
	if inputs['do_clock_search'] == True:
		cs = "clock_search/"
	else:
		cs = "correlation/"
	if recorrelate == True:
		rc = 1
	else:
		rc = 0
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
				if len(vexfile['SCHED'][scan_c]['source']) > 10:
					srcs = ", ".join(vexfile['SCHED'][scan_c]['source'][0:10]) + ' + ... (total: %d sources)'%len(vexfile['SCHED'][scan_c]['source'])
				else:
					srcs = ", ".join(vexfile['SCHED'][scan_c]['source'])
				print('Making the following correlator scans: %s for sources: %s'%(scan_c,srcs))
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
	return corr_files