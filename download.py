
import pandas as pd


class Subsetter:

    def __init__(self, layers, flags, keepevery=1, predicate=None):
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

        # drop potentially most data
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
        if beams == "all":
            beams = [k for k in granule.keys() if k.startswith("BEAM")]
        return pd.concat([self.subsetbeam(granule, b) for b in beams])


# TODO: figure out automatic authentication
"""def gedi_request(url, user, pwd):
    pm = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    pm.add_password(None, "https://urs.earthdata.nasa.gov", user, pwd)
    cookie_jar = CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPBasicAuthHandler(pm),
        urllib.request.HTTPCookieProcessor(cookie_jar)
    )
    urllib.request.install_opener(opener)
    myrequest = urllib.request.Request(url)
    return urllib.request.urlopen(myrequest)


def subseturl(url, setter: Subsetter, user, pwd, beams="all") -> pd.DataFrame:
    r = gedi_request(url, user, pwd)
    r.begin()
    with BytesIO() as filelike:
        try:
            while True:
                chunk = r.read()
                if chunk:
                    filelike.write(chunk)
                else:
                    break
            granule = h5py.File(filelike, 'r')
            return setter.subsetgranule(granule, beams=beams)
        except Exception as e:
            print(f"An Exception of type {type(e)} caused failed download from {url}")
            return e
"""
