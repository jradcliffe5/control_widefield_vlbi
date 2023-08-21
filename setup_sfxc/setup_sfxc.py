import os, sys, json, ast, re, inspect
import numpy as np

filename = inspect.getframeinfo(inspect.currentframe()).filename
sys.path.append(os.path.dirname(os.path.realpath(filename)+'/../vex'))

import vex

def flatten_extend(matrix):
    flat_list = []
    for row in matrix:
        flat_list.extend(row)
    return flat_list

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


try:
	i = sys.argv.index("-c") + 2
except:
	i = 1
	pass
inputs = headless(sys.argv[i])
bb_loc = ast.literal_eval(inputs['baseband_location'])
o_dir = ast.literal_eval(inputs["output_dir"])
ctrl_file = {}


for i in ["exper_name","vex_file","cross_polarize","number_channels","slices_per_integration","stop","start","setup_station","integr_time","message_level","slices_per_integration","LO_offset","multi_phase_center","sub_integr_time","fft_size_correlation"]:
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

corr_chans = []
for i in range(ast.literal_eval(inputs['correlator_channels'])):
	corr_chans.append('CH%02d'%(i+1))
ctrl_file["channels"] = corr_chans


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

## Make data sources
data_sources = {}
for i in ss.keys():
	for k,j in enumerate(ss[i]):
		if j in data_sources:
			data_sources[j] = data_sources[j]+['file://%s/%s'%(bb_loc,ss_s[i][k])]
		else:
			data_sources[j] = ['file://%s/%s'%(bb_loc,ss_s[i][k])]
ctrl_file['data_sources'] = data_sources


rmfiles(["%s.ctrl"%ast.literal_eval(inputs['exper_name'])])
with open("%s.ctrl"%ast.literal_eval(inputs['exper_name']), "w") as outfile:
    json.dump(ctrl_file, outfile)