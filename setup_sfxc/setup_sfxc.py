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

if inputs['correlator_mode'] == 'clocksearch':
	inputs['do_clock_search'] = True
	recorrelate=False
	postprocessonly=False
elif inputs['correlator_mode'] == 'correlate':
	inputs['do_clock_search'] = False
	recorrelate=False
	postprocessonly=False
elif inputs['correlator_mode'] == 'postprocess':
	inputs['do_clock_search'] = False
	recorrelate=False
	postprocessonly=True
elif inputs['correlator_mode'] == 'recorrelatetargets':
	inputs['do_clock_search'] = False
	recorrelate=True
	postprocessonly=False
else:
	print('Correlator mode invalid. Can only be one of clocksearch|correlate|postprocess|recorrelatetargets')
	sys.exit()

### MAKE CTRL FILE TEMPLATE FOR WHOLE CORRELATION ###
ctrl_file, ss, ss_s = build_master_ctrl_file(inputs=inputs,
											 vexfile=vexfile)

if inputs['multi_cluster'] == True:
	if os.path.exists(inputs['cluster_config']) == False:
		raise Exception('Cluster configuration file (%s) does not exist'%inputs['cluster_config'])
	if inputs['do_clock_search'] == True:
		raise Exception('Multi cluster not supported for clock searches, please turn it off.')
	cluster_params = load_json('%s'%inputs['cluster_config'])
	c_names = ['localhost']+list(cluster_params.keys())
	if not (len(c_names)==len(inputs['correlation_share_ratios'])):
		raise AssertionError('Ratios and number of clusters are mismatched')
	scans = split_scans(scan_dict=ss,ratios=inputs['correlation_share_ratios'],names=c_names)
else:
	c_names = ['localhost']
	ratios=[1]
	cluster_params = {}
	scans = {'localhost':ss}

save_json('%s/logs/scans.json'%o_dir,ss)
save_json('%s/logs/datasources.json'%o_dir,ss_s)

for i in c_names:
	if i !="localhost":
		l2r_commands = ["#!/bin/bash"]
	
	l2r_mkdir, l2r_copy, cs = build_directory_structure(exper=exper,
									 o_dir=o_dir,
									 bb_loc=bb_loc,
									 recorrelate=recorrelate,
									 clocksearch=inputs['do_clock_search'],
									 scans=scans[i],
									 postprocessonly=postprocessonly,
									 data_sources=ss_s,
									 scp=inputs['singularity_container_path'],
									 vex_loc=inputs['vex_file'],
									 cluster_name=i,
									 cluster_config=cluster_params)
	if i !="localhost":
		l2r_commands.append('if [ \"$1\" = \"A\" ]; then')
		l2r_commands.extend(l2r_mkdir)
		l2r_commands.append('fi')
	
	l2r_copy.extend(generate_correlator_environment(exper=exper,
									vexfile=vexfile,
									scans=scans[i],
									datasources=ss_s,
									cluster_name=i,
									cluster_config=cluster_params,
									inputs=inputs,
									ctrl_file=ctrl_file))
	if i !="localhost":
		l2r_commands.append('if [ \"$1\" = \"B\" ]; then')
		if cluster_params[i]["cluster_specification"]["job_manager"] == 'slurm':
			l2r_commands.append('sbatch -W %s/job_run_sfxc_%s.%s'%(cluster_params[i]["correlation_dir"],i,cluster_params[i]["cluster_specification"]["job_manager"]))
		elif cluster_params[i]["cluster_specification"]["job_manager"] == 'pbs':
			l2r_commands.append('qsub -sync y %s/job_run_sfxc_%s.%s'%(cluster_params[i]["correlation_dir"],i,cluster_params[i]["cluster_specification"]["job_manager"]))
		elif cluster_params[i]["cluster_specification"]["job_manager"] == 'bash':
			l2r_commands.append('bash %s/job_run_sfxc_%s.%s'%(cluster_params[i]["correlation_dir"],i,cluster_params[i]["cluster_specification"]["job_manager"]))
		else:
			raise Exception('Job manager not recognised. Software supports only SLURM, PBS Pro and bash')
		l2r_commands.append('fi')
		write_job(step='run_l2r_%s'%i,commands=l2r_commands,job_manager='bash',write='w')
		write_job(step='run_copy_%s'%i,commands=l2r_copy,job_manager='bash',write='w')


corr_files = list_correlation_outputs(scans=ss,
									exper=exper,
									cs=cs,
									vexfile=vexfile,
									calibrator=inputs['calibrator_target'])

if postprocessonly==True:
	commands = []
	print('Building script for conversion to measurement sets')
	for j,i in enumerate(list(corr_files.keys())):
		print('Source %s .. done'%i)
		commands.append('%s %s -o %s/post_processing/%s_%d_1.ms 2>&1 | tee %s/logs/j2ms2_%s.log &'%(inputs["j2ms2_exec"]," ".join(corr_files[i]),o_dir,exper,j+1,o_dir,i))
	commands.reverse()
	commands[-1] = commands[-1].split(' &')[0]
	write_job(step='run_j2ms2',commands=commands,job_manager='bash',write='w')

	print('Building script for flagging of low correlator weights')
	commands = ['parallel -eta -j 40 %s sfxc_helperscripts/post_processing/flag_weights.py {} %.3f True ::: %s/post_processing/*.ms 2>&1 | tee %s/logs/flag_weights.log'%(inputs['casa_exec'],inputs['flag_threshold'],o_dir,o_dir)]
	commands.append('mv *.log logs')
	write_job(step='run_flag_data',commands=commands,job_manager='bash',write='w')

	print('Building script for conversion to FITS-IDI files')
	commands = []
	for j,i in enumerate(list(corr_files.keys())):
		print('Source %s .. done'%i)
		commands.append('%s %s/post_processing/%s_%d_1.ms %s/post_processing/%s_%d_1.IDI 2>&1 | tee %s/logs/tconvert_%s.log &'%(inputs["tconvert_exec"],o_dir,exper,j+1,o_dir,exper,j+1,o_dir,j+1))
	commands.reverse()
	commands[-1] = commands[-1].split(' &')[0]
	print('Moving phase centre with calibrators for calibration')
	commands.append('cp -rv %s/post_processing/%s_1_1.IDI* %s/calibration/raw_uv/'%(o_dir,exper,o_dir))
	write_job(step='run_tconvert',commands=commands,job_manager='bash',write='w')