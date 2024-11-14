import os
import pathlib
import shutil
import sys

import numpy as np
import OpenVisus as ov
import xarray as xr

ds_object = sys.argv[1]
output_dir = sys.argv[2]


## HACK: Use our token instead of the one from the AP.

cred_dir = pathlib.Path(os.environ.get("_CONDOR_CREDS", "."))
shutil.copyfile("osdf.token", cred_dir / "scitokens.use")


## Determine the field name.

filename, extension = os.path.splitext(ds_object)
file_parts = filename.split("_")

if file_parts[-1].startswith("v"):
    year = int(file_parts.pop(-2))
else:
    year = int(file_parts.pop(-1))

field = file_parts[0]
field_name = "_".join(file_parts)


## Define the conversion's parameters.

visus_idx_file = f"{output_dir}/{ds_object}.idx"
visus_data_dir = f"{output_dir}/{ds_object}"
arco = "2mb"


## Clean up data from a previous run (helpful for local testing).

shutil.rmtree(visus_data_dir, ignore_errors=True)


## Create and compress the index.

ds = xr.open_dataset(ds_object, group="/", mask_and_scale=False)
data = ds[field][...].values
vmin, vmax = np.min(data), np.max(data)

ov_field = ov.Field.fromString(
    f"{field_name} {str(data.dtype)} format(row_major) min({vmin}) max({vmax})"
)

print(f"data.shape: {data.shape}")
D, H, W = data.shape
D = min(D, 364)  # look at only 365 days (0..364)

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
    db.write(slice, time=year * 365 + Z, field=field_name)

db.compressDataset(["zip"])
