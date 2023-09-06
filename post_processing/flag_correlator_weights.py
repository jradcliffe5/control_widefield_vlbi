from casatasks import *


filename = inspect.getframeinfo(inspect.currentframe()).filename
sys.path.append(os.path.dirname(os.path.realpath(filename)))

try:
	i = sys.argv.index("-c") + 2
except:
	i = 1
	pass

msfile = sys.argv[i]
threshold=float(sys.argv[i+1])

flagdata(vis=msfile,mode='clip',clipminmax=[threshold,1e9], datacolumn='WEIGHT',flagbackup=False)