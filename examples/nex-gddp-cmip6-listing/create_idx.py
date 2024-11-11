#!/usr/bin/env python3

import os

import OpenVisus as ov

ARCO = "2mb"
DIMS = [1440, 600]
IDX_DIRECTORY = "."
MAIN_IDX_NAME = "nex-gddp-cmip6"
MAIN_IDX_FILE = f"{MAIN_IDX_NAME}.idx"
LAT_MIN = 0.125
LAT_MAX = 359.875
LON_MIN = -59.88
LON_MAX = 89.88


def get_field_names():

    field_names = []

    with open("nex-gddp-cmip6.txt", mode="r", encoding="utf-8") as fp:
        for line in fp.readlines():

            # Skip lines that cannot possibly contain an object of interest.

            if "/" not in line:
                continue

            _, _, _, url = line.split()
            _, model, scenario, variant, variable, filename = url.split("/")
            filename, extension = os.path.splitext(filename)

            if extension != ".nc":
                continue

            file_parts = filename.split("_")

            if len(file_parts) == 8:
                _, _, _, _, _, variant_2, _, version = file_parts
            elif len(file_parts) == 7:
                _, _, _, _, _, variant_2, _ = file_parts
            else:
                continue  # skip filenames with an unexpected structure

            field_name = f"{variable}_day_{model}_{scenario}_{variant}_{variant_2}"

            if len(file_parts) == 8:
                field_name += f"_{version}"

            if field_name not in field_names:
                field_names.append(field_name)

    return sorted(field_names)


def create_main_idx():

    ov_fields = []

    for field_name in get_field_names():
        field = ov.Field(field_name, "float32")
        field.default_compression = "zip"
        ov_fields.append(field)

    idx = ov.CreateIdx(
        url=os.path.join(IDX_DIRECTORY, MAIN_IDX_FILE),
        fields=ov_fields,
        compression="raw",
        time=[1950 * 365, 2100 * 365 + 364, "%d/"],
        dims=DIMS,
        arco=ARCO,
        filename_template=f"./{MAIN_IDX_NAME}/%04x.bin",
        blocksperfile=1,
        physic_box=ov.BoxNd.fromString(f"{LAT_MIN} {LAT_MAX} {LON_MIN} {LON_MAX}"),
    )

    return idx


if __name__ == "__main__":
    create_main_idx()
