
import matplotlib.pyplot as plt
import h5py

from download import Subsetter


layers = ["lat_lowestmode", "lon_lowestmode", "beam", "rh"]
flags = {
    "quality_flag": 1,
    "degrade_flag": 0
}
def pred(row):
    return row["lon_lowestmode"] > -100


setter = Subsetter(layers, flags, keepevery=100, predicate=pred)

# file too large to push to git
granule = h5py.File("GEDI02_A_2019108185228_O01971_03_T00922_02_003_01_V002.h5", 'r')
df = setter.subsetgranule(granule)

# TODO: authenticate
# url = "https://e4ftl01.cr.usgs.gov//GEDI_L1_L2/GEDI/GEDI02_A.002/2019.04.18/GEDI02_A_2019108185228_O01971_03_T00922_02_003_01_V002.h5"
# TODO: retrieve pwd from local file so it doesn't get published to github
# df = subseturl(url, setter, user="fcseidl", pwd="not my real pwd")

for b in range(1, 9):
    bidx = (df["beam"] == b)
    plt.scatter(
        df["lon_lowestmode"][bidx], df["lat_lowestmode"][bidx],
        s=2, label="beam %d" % b
    )

plt.legend()
plt.show()

