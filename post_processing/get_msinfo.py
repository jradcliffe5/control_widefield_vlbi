import casatools
import numpy as np
from scipy.constants import c as speed_light
import json, collections, sys, ast
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

def get_ms_info(msfile):
	tb = casatools.table()
	ms = casatools.ms() 
	msinfo={}
	## antenna information
	tb.open('%s/ANTENNA'%msfile)
	ants = tb.getcol('NAME')
	ant={}
	ant['anttoID'] =dict(zip(ants, np.arange(0,len(ants),1)))
	ant['IDtoant'] = dict(zip(np.arange(0,len(ants),1).astype(str),ants))
	msinfo['ANTENNAS']=ant
	tb.close()

	## get spw information
	tb.open('%s/SPECTRAL_WINDOW'%msfile)
	spw={}
	spw['nspws'] = len(tb.getcol('TOTAL_BANDWIDTH'))
	spw['bwidth'] = np.sum(tb.getcol('TOTAL_BANDWIDTH'))
	spw['spw_bw'] = spw['bwidth']/spw['nspws']
	spw['freq_range'] = [tb.getcol('CHAN_FREQ')[0][0],tb.getcol('CHAN_FREQ')[0][0]+spw['bwidth']]
	spw['cfreq'] = np.average(spw['freq_range'])
	if ((np.max(tb.getcol('CHAN_WIDTH')) == np.min(tb.getcol('CHAN_WIDTH')))&(np.max(tb.getcol('NUM_CHAN')) == np.min(tb.getcol('NUM_CHAN')))) == True:
		spw['same_spws'] = True
		spw['nchan'] = np.max(tb.getcol('NUM_CHAN'))
	else:
		spw['same_spws'] = False
		spw['nchan'] = tb.getcol('NUM_CHAN')
	if spw['same_spws'] == True:
		spw['chan_width'] = tb.getcol('CHAN_WIDTH')[0][0]
	else:
		spw['chan_width'] = np.average(tb.getcol('CHAN_WIDTH'))
	tb.close()
	tb.open('%s/POLARIZATION'%msfile)
	spw['npol'] = tb.getcol('NUM_CORR')[0]
	polariz = tb.getcol('CORR_TYPE').flatten()
	ID_to_pol={'0': 'Undefined',
			 '1': 'I',
			 '2': 'Q',
			 '3': 'U',
			 '4': 'V',
			 '5': 'RR',
			 '6': 'RL',
			 '7': 'LR',
			 '8': 'LL',
			 '9': 'XX',
			 '10': 'XY',
			 '11': 'YX',
			 '12': 'YY',
			 '13': 'RX',
			 '14': 'RY',
			 '15': 'LX',
			 '16': 'LY',
			 '17': 'XR',
			 '18': 'XL',
			 '19': 'YR',
			 '20': 'YL',
			 '21': 'PP',
			 '22': 'PQ',
			 '23': 'QP',
			 '24': 'QQ',
			 '25': 'RCircular',
			 '26': 'LCircular',
			 '27': 'Linear',
			 '28': 'Ptotal',
			 '29': 'Plinear',
			 '30': 'PFtotal',
			 '31': 'PFlinear',
			 '32': 'Pangle'}
	pol2=[]
	for i,j in enumerate(polariz):
		pol2.append(ID_to_pol[str(j)])
	spw['spw_pols'] = pol2
	tb.close()
	msinfo['SPECTRAL_WINDOW'] = spw
	## Get field information
	tb.open('%s/FIELD'%msfile)
	fields = tb.getcol('NAME')
	field = {}
	field['fieldtoID'] =dict(zip(fields, np.arange(0,len(fields),1)))
	field['IDtofield'] = dict(zip(np.arange(0,len(fields),1).astype(str),fields))
	tb.close()
	## scans
	ms.open(msfile)
	scans = ms.getscansummary()
	ms.close()
	scan = {}
	for i in list(scans.keys()):
		fieldid = scans[i]['0']['FieldId']
		if fieldid not in list(scan.keys()):
			scan[fieldid] = [i]
		else:
			vals = scan[fieldid]
			scan[fieldid].append(i)
	msinfo['SCANS'] = scan
	## Get telescope_name
	tb.open('%s/OBSERVATION'%msfile)
	msinfo['TELE_NAME'] = tb.getcol('TELESCOPE_NAME')[0]
	tb.close()
	image_params = {}
	high_freq = spw['freq_range'][1]
	
	ms.open(msfile)
	f = []
	indx = []
	for i in field['fieldtoID'].keys():
		ms.selecttaql('FIELD_ID==%s'%field['fieldtoID'][i])
		try:
			max_uv = ms.getdata('uvdist')['uvdist'].max()
			image_params[i] = ((speed_light/high_freq)/max_uv)*(180./np.pi)*(3.6e6/5.)
			f.append(i)
			indx.append(field['fieldtoID'][i])
		except:
			pass
		ms.reset()
	ms.close()
	field = {}
	field['fieldtoID'] =dict(zip(f, indx))
	field['IDtofield'] =dict(zip(np.array(indx).astype(str),f))
	msinfo['FIELD'] = field
	msinfo["IMAGE_PARAMS"] = image_params
	
	return msinfo

try:
	i = sys.argv.index("-c") + 2
except:
	i = 1
	pass

msdata = sys.argv[i]
verbose = ast.literal_eval(sys.argv[i+1])

msinfo = get_ms_info(msdata)
if verbose == True:
	print(json.dumps(msinfo, sort_keys=True, indent=4, cls=NpEncoder))
save_json("%s_msinfo.json"%(msdata.split('.ms')[0]),msinfo)