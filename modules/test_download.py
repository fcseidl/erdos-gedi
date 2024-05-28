"""
This demo obtains data from the granule GEDI02_A_2019108185228_O01971_03_T00922_02_003_01_V002,
dropping low-quality and degraded shots, as well as any shots west of 100W longitude. Shot locations
are scatterplotted, colored by beam, in a cylindrical projection. Note that distinguishing beams
requires zooming in.
"""

import matplotlib.pyplot as plt

from subset import Subsetter
from download import Downloader

"""Create a Subsetter object to download only the data we desire."""

layers = ["lat_lowestmode", "lon_lowestmode", "beam", "rh"]
flags = {
    "quality_flag": 1,
    "degrade_flag": 0
}
def pred(row):
    return row["lon_lowestmode"] > -100

setter = Subsetter(layers, flags, keepevery=100, predicate=pred)


"""We can run the Subsetter locally on the pre-downloaded granule."""

# granule = h5py.File("GEDI02_A_2019108185228_O01971_03_T00922_02_003_01_V002.h5", 'r')
# df = setter.subsetgranule(granule)


"""
Or we can use a Downloader to temporarily store the granule data for subsetting. 
This way, none of the data we'll eventually discard is ever written to disk.
"""

url = "https://e4ftl01.cr.usgs.gov//GEDI_L1_L2/GEDI/GEDI02_A.002/2019.04.18/GEDI02_A_2019108185228_O01971_03_T00922_02_003_01_V002.h5"
df = Downloader().process_granule(url, setter.subsetgranule)


for b in range(1, 9):
    bidx = (df["beam"] == b)
    plt.scatter(
        df["lon_lowestmode"][bidx], df["lat_lowestmode"][bidx],
        s=2, label="beam %d" % b
    )

plt.legend()
plt.show()

