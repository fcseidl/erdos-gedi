
import pandas as pd
from shapely.geometry import Point


class Subsetter:

    def __init__(self, layers, flags, keepevery=1, predicate=None):
        """
        Define constraints for subsetting GEDI granules.

        :param layers: List of names of GEDI product layers to keep, e.g., randomly,
                        "rx_processing_a6/ancillary/pulse_sep_thresh"
        :param flags: Dictionary mapping flag layers to values which will be enforced for those flags, e.g.,
                        {"quality_flag": 1} will result in low-quality shots being dropped. Note that all used
                        flags are dropped from the DataFrames produced, since they are constant in the data kept.
        :param keepevery: Downsampling factor. For instance, if keepevery == 10, then only every 10th
                            shot is considered.
        :param predicate: Boolean function used to determine shots to drop from the data. This function may assume
                            only that its input has the requested layers as its keys.
        """
        self.layers = layers
        self.flags = flags
        self.ke = keepevery
        self._columns = list(layers) + list(flags.keys())
        self._pred = predicate

    def predicate(self, row) -> bool:
        try:
            return self._pred(row)
        except KeyError as e:
            print(e.message)
            return False

    def subsetbeam(self, granule, beam) -> pd.DataFrame:
        # load all needed columns into DataFrame
        bdf = pd.DataFrame({
            layer: list(granule[beam + '/' + layer])
            for layer in self._columns
        })

        # downsample data
        bdf = bdf[0:bdf.shape[0]:self.ke]

        # drop flagged rows, then flag columns
        for k in self.flags.keys():
            bdf = bdf[bdf[k] == self.flags[k]].drop(k, axis=1)

        # drop rows failing predicate
        if self._pred is not None:
            index = [self.predicate(row) for _, row in bdf.iterrows()]
            bdf = bdf[index]

        return bdf

    def subsetgranule(self, granule, beams="all") -> pd.DataFrame:
        """
        Subset a GEDI granule according to this object's constraints.

        :param granule: An h5py.File object representing a GEDI granule.
        :param beams: Optional list of beam names to subset. If not specified, all beams are used.
        :return: a pandas DataFrame containing all shots fitting subsetting constraints, with a column
                    for each requested layer.
        """
        if beams == "all":
            beams = [k for k in granule.keys() if k.startswith("BEAM")]
        return pd.concat([self.subsetbeam(granule, b) for b in beams])


class InBounds:

    def __init__(self, bounds):
        """
        Define a predicate which fails shots whose lon_lowestmode and lat_lowestmode are out of bounds.
        bounds object should have a contains() method which accepts a shapely Point object, e.g., if
        bounds is a shapely Polygon.
        """
        self._bounds = bounds

    def __call__(self, shot):
        return self._bounds.contains(
            Point(shot["lon_lowestmode"], shot["lat_lowestmode"]))

