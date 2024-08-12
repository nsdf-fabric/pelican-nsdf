import datetime
import os
import shutil
import subprocess
import sys

import numpy as np
import OpenVisus as ov
import openvisuspy
import xarray as xr

destination_dir = sys.argv[1]
ds_object = sys.argv[2]
osdf_destination_loc = sys.argv[3]
pelican_token = sys.argv[4]


## Parse metadata.

ds_parameters = os.path.splitext(ds_object)[0].split("_")
version = ds_parameters.pop(-1) if ds_parameters[-1][0] == "v" else None
year = int(ds_parameters.pop(-1))
if version:
    ds_parameters.append(version)
field, day, model, scenario, r, g = ds_parameters
ds_parameters = "_".join(ds_parameters)


## Define the conversion's parameters.

visus_idx_file = f"{destination_dir}/visus.idx"
visus_data_dir = os.path.splitext(visus_idx_file)[0]
arco = "2mb"


## Clean up data from a previous run (helpful for local testing).

shutil.rmtree(visus_data_dir, ignore_errors=True)


## Create and compress the index.

ds = xr.open_dataset(ds_object, group="/", mask_and_scale=False)
data = ds[field][...].values
vmin, vmax = np.min(data), np.max(data)

ov_field = ov.Field.fromString(
    f"{ds_parameters} {str(data.dtype)} format(row_major) min({vmin}) max({vmax})"
)

D, H, W = data.shape
assert D == 365  # ensure that the NetCDF file covers 365 days

db = ov.CreateIdx(
    url=visus_idx_file,
    dims=[W, H],
    fields=[ov_field],
    compression="raw",
    arco=arco,
    time=[
        year * 365,
        year * 365 + D,
        "time_%d/",
    ],
)

for Z in range(D):  # each Z is a day (offset from the year)
    slice = data[Z, ...]
    db.write(slice, time=year * 365 + Z, field=ds_parameters)

db.compressDataset(["zip"])


## Write the index into OSDF.

# TODO: Incorporate this into HTCondor's file transfer mechanism.

proc = subprocess.run(
    [
        "pelican",
        "--debug",
        "object",
        "put",
        "-r",
        "-t",
        pelican_token,
        destination_dir,
        osdf_destination_loc,
    ],
    capture_output=True,
)

sys.stdout.write(proc.stdout.decode("utf-8"))
sys.stdout.flush()
sys.stderr.write(proc.stderr.decode("utf-8"))
sys.stderr.flush()
sys.exit(proc.returncode)
