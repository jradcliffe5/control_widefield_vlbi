#!/usr/bin/env python3
"""Flag visibilities with weights below the provided threshold.

Usage: flag_weights.py msdata threshold
Options:
    msdata : str          MS data set containing the data to be flagged.
    threshold : float     Visibilities with a weight below the specified
                          value will be flagged. Must be positive.

Version: 3.4v-pretoria
Date: 12/2023
- Modfied by Jack Radcliffe
- reverted to casacore

version 3.3.1 changes (Jul 2023)
- Written by Benito Marcote (marcote@jive.eu)
- chunkert chunk value changed to 100 (optimal IO speed)
version 3.3 changes (Jan 2023)
- Richer progress bar
version 3.2 changes (Mar 2021)
- w > 0 condiction moved to > 0.001 to include possible weights at the ~1e-5 level.
version 3.1 changes (Mar 2020)
- Progress bar added.
version 3.0 changes (Apr 2019)
- Refactoring code (thanks to Harro).
version 2.0 changes
- Major revision. Now it does not modify the weights anymore. Instead, it
  flags those data with weights below the given threshold by modifying the
  FLAG table.
- Small change in print messages to show '100%' instead of '1e+02%' in certain
  cases.
version 1.4 changes
- Now it also reports the percentage or data that were different from
  zero and will be flagged (not only the total data as before).
version 1.3 changes
- Minor fixes (prog name in optparse info).
version 1.2 changes
- Minor fixes.
version 1.1 changes
- Added option -v that allows you to just get how many visibilities will
  be flagged (but without actually flagging the data).

"""

import sys
import numpy as np
#from rich import progress
from casatools import table as tb

__version__ = '3.3.1'
help_msdata = 'Measurement set containing the data to be corrected.'
help_threshold = 'Visibilities with a weight below this value will be flagged. Must be positive.'
help_v = 'Only checks the visibilities to flag (do not flag the data).'

try:
	i = sys.argv.index("-c") + 2
except:
	i = 1
	pass

msdata = sys.argv[i]
threshold = float(sys.argv[i+1])


assert threshold > 0.0

def chunkert(f, l, cs, verbose=True):
    while f<l:
        n = min(cs, l-f)
        yield (f, n)
        f = f + n


percent = lambda x, y: (float(x)/float(y))*100.0

print(msdata)
ms = tb.open(msdata, nomodify=False)
total_number = 0
flagged_before, flagged_after = (0, 0)
flagged_nonzero, flagged_nonzero_before, flagged_nonzero_after = (0, 0, 0)
# WEIGHT: (nrow, npol)
# WEIGHT_SPECTRUM: (nrow, npol, nfreq)
# flags[weight < threshold] = True
weightcol = 'WEIGHT_SPECTRUM' if 'WEIGHT_SPECTRUM' in ms.colnames() else 'WEIGHT'
transpose = (lambda x:x) if weightcol == 'WEIGHT_SPECTRUM' else (lambda x: x.transpose((1, 0, 2)))
for (start, nrow) in chunkert(0, len(ms), 100):
    # shape: (nrow, npol, nfreq)
    flags = transpose(ms.getcol("FLAG", startrow=start, nrow=nrow))
    total_number += np.product(flags.shape)
    # count how much data is already flagged
    flagged_before += np.sum(flags)
    # extract weights and compute new flags based on threshold
    weights = ms.getcol(weightcol, startrow=start, nrow=nrow)
    # how many non-zero did we flag
    flagged_nonzero_before = np.logical_and(flags, weights > 0.001)
    # join with existing flags and count again
    flags = np.logical_or(flags, weights < threshold)
    flagged_after += np.sum(flags)
    flagged_nonzero_after = np.logical_and(flags, weights > 0.001)
    # Saving the total of nonzero flags (in this and previous runs)
    # flagged_nonzero += np.sum(np.logical_xor(flagged_nonzero_before, flagged_nonzero_after))
    flagged_nonzero += np.sum(flagged_nonzero_after)
    # one thing left to do: write the updated flags to disk
    #flags = ms.putcol("FLAG", flags.transpose((1, 0 , 2)), startrow=start, nrow=nrow)
    if verbose:
        ms.putcol("FLAG", transpose(flags), startrow=start, nrow=nrow)

print("\nGot {0:11} visibilities".format(total_number))
print("Got {0:11} visibilities to flag using threshold {1}\n".format(flagged_after-flagged_before,
                                                                                threshold))
print("{0:.2f}% total vis. flagged ({2:.2f}% to flag in this execution).\n{1:.2f}% data with non-zero weights flagged.\n".format(percent(flagged_after, total_number), percent(flagged_nonzero, total_number), percent(flagged_after-flagged_before, total_number)))
ms.close()

if verbose:
    print('Done.')
else:
    print('Flags have not been applied.')

