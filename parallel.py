
import os.path
from typing import Iterable
from tqdm.contrib.concurrent import process_map

from download import Downloader
from subset import Subsetter


class ParallelDownloadAndSubset:

    def __init__(
            self,
            loader: Downloader,
            setter: Subsetter,
            savedir: str,
            beams="all"
    ):
        """
        Create an object to download a subset of the data from a group of GEDI granules.

        :param loader: Downloader object to remotely access GEDI data.
        :param setter: Subsetter object defining constraints on data of interest.
        :param savedir: Path to directory in which to save resulting data in CSV format. The data kept from each granule is
                        stored in a separate csv file, named e.g.
                        'GEDI02_A_2019108002012_O01959_01_T03909_02_003_01_V002.csv'.
                        NOTE THAT PREEXISTING FILES WILL NOT BE OVERWRITTEN!!!
        :param beams: Optional list of beam names to subset. If not specified, all beams are used.
        """
        self.loader = loader
        self.setter = setter
        self.savedir = savedir
        self.beams = beams

    def _process_url(self, url):
        granule_id = url[url.rindex('/') + 1:url.rindex('.')]
        savefile = os.path.join(self.savedir, granule_id + ".csv")
        if not os.path.exists(savefile):
            df = self.loader.process_granule(
                url, lambda f: self.setter.subsetgranule(f, self.beams))
            df.to_csv(savefile)

    def process_urls(self, urls: Iterable[str], **kwargs) -> None:
        """
        Download and subset data from a list of GEDI granule urls, like this one:
        'https://e4ftl01.cr.usgs.gov/GEDI/GEDI02_A.002/2019.04.18/GEDI02_A_2019108002012_O01959_01_T03909_02_003_01_V002.h5'

        :param urls: Web address of each GEDI granule.
        :param kwargs: Passed to concurrent.futures.ProcessPoolExecutor.map(). Note that the number of parallel
                    processes is given by the max_workers keyword.
        """
        process_map(self._process_url, urls, **kwargs)
