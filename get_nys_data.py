"""
This script downloads data from L2A shots in New York State. Execution may take
hours or days, depending on the amount parallelism employed. However, progress
is saved continually, and the script picks up where it left off if restarted.
Also note that the progress bar slightly lags true progress.
"""

import sys
from pickle import load
from multiprocessing import freeze_support

from download import Downloader
from subset import Subsetter, InBounds
from parallel import ParallelDownloadAndSubset

if __name__ == "__main__":
    freeze_support()  # or else bad things happen

    # specify number of parallel processes via command line
    nproc = int(sys.argv[1])

    loader = Downloader()

    # save these features
    layers = [
        "beam",
        "channel",
        "lat_lowestmode",
        "lon_lowestmode",
        "elev_lowestmode",
        "delta_time",
        "rh",
        "land_cover_data/landsat_water_persistence",
        "land_cover_data/landsat_treecover",
        "land_cover_data/region_class",
        "land_cover_data/urban_proportion",
        "land_cover_data/urban_focal_window_size",
        "shot_number"
    ]

    # only save shots where these flags have the specified values
    flags = {
        "quality_flag": 1,
        "degrade_flag": 0,
        "elevation_bias_flag": 0
    }

    # only save shots inside New York State
    with open("nys_simple_boundaries.pickle", "rb") as polygon:
        bounds = load(polygon)
    in_nys = InBounds(bounds)

    setter = Subsetter(layers, flags, keepevery=10, predicate=in_nys)

    # parallel processing of all granules intersecting NYS bounding box
    with open("GEDI02_A_002_GranuleList_20240516130017.txt") as granule_list:
        urls = [line.rstrip('\n') for line in granule_list]
    print(f"Downloading and subsetting {len(list(urls))} granules over {nproc} processes...")
    ParallelDownloadAndSubset(loader, setter, "data").process_urls(
        urls[100:],
        max_workers=nproc,
        chunksize=5
    )
