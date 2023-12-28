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
for j in inputs.keys():
	inputs[j] = ast.literal_eval(inputs[j])
exper = inputs['exper_name']
bb_loc = inputs['baseband_location']
o_dir = inputs["output_dir"]
vexfile = vex.Vex(inputs["vex_file"])
sfxc_exec = inputs['sfxc_exec']
produce_html_plot_exec = inputs['produce_html_plot_exec']
recorrelate = inputs['recorrelate_targets']

### MAKE CTRL FILE TEMPLATE FOR WHOLE CORRELATION ###
ctrl_file, ss, ss_s = build_master_ctrl_file(inputs=inputs,
											 vexfile=vexfile)

build_directory_structure(recorrelate=recorrelate,
						  clocksearch=inputs['do_clock_search'],
						  scans=ss)

corr_files = generate_correlator_environment(exper=exper,
												vexfile=vexfile,
												scans=ss,
												datasources=ss_s,
												cluster_name="localhost",
												inputs=inputs,
												ctrl_file=ctrl_file,
												recorrelate=recorrelate)

commands = []
print('Building script for conversion to measurement sets')
ms_output = inputs['ms_output']
for i in list(corr_files.keys()):
	print('Source %s .. done'%i)
	if ms_output =="":
		commands.append('%s %s -o %s/%s.ms 2>&1 | tee %s/logs/j2ms2_%s.log &'%(inputs["j2ms2_exec"]," ".join(corr_files[i]),o_dir,i,o_dir,i))
	else:
		commands.append('%s %s -o %s/%s.ms 2>&1 | tee %s/logs/j2ms2_%s.log &'%(inputs["j2ms2_exec"]," ".join(corr_files[i]),ms_output,i,o_dir,i))
commands[-1] = commands[-1].split(' &')[0]
if ms_output !="":
	for i in list(corr_files.keys()):
			commands.append('cp -rv %s/%s.ms %s &'%(ms_output,i,o_dir))
	commands[-1] = commands[-1].split(' &')[0]
write_job(step='run_j2ms2',commands=commands,job_manager='bash',write='w')
print('Building script for flagging of low correlator weights')
commands = ['parallel -eta -j 40 %s sfxc_helperscripts/post_processing/flag_weights.py {} %.3f ::: *.ms 2>&1 | tee %s/logs/flag_weights.log'%(inputs['casa_exec'],inputs['flag_threshold'],o_dir)]
write_job(step='run_flag_data',commands=commands,job_manager='bash',write='w')