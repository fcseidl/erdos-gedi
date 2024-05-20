"""
Acknowledgement: this module borrows code written by Erick Verleye and Frank Seidl in
https://github.com/earthlab/BioExtremes/blob/main/GEDI/api.py
"""

import os
from io import BytesIO
import certifi
from typing import Callable
from http.cookiejar import CookieJar
import urllib
import h5py


class Downloader:
    """
    This class downloads GEDI granules for subsetting. Note that the only way to access the
    contents of a granule is through the process_in_memory_file() method. This is to discourage writing unprocessed
    files to disk, since the raw data is large and mostly not useful.
    """

    def __init__(self):
        """Takes Earth Data login credentials from 'BEX_USER' and 'BEX_PWD' environment variables."""
        self._username = os.environ['BEX_USER']
        self._password = os.environ['BEX_PWD']

        # resolve potential issue with SSL certs
        ssl_cert_path = certifi.where()
        if 'SSL_CERT_FILE' not in os.environ or os.environ['SSL_CERT_FILE'] != ssl_cert_path:
            os.environ['SSL_CERT_FILE'] = ssl_cert_path
        if 'REQUESTS_CA_BUNDLE' not in os.environ or os.environ['REQUESTS_CA_BUNDLE'] != ssl_cert_path:
            os.environ['REQUESTS_CA_BUNDLE'] = ssl_cert_path

    def _request_raw_data(self, url: str):
        pm = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        pm.add_password(None, "https://urs.earthdata.nasa.gov", self._username, self._password)
        cookie_jar = CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.HTTPBasicAuthHandler(pm),
            urllib.request.HTTPCookieProcessor(cookie_jar)
        )
        urllib.request.install_opener(opener)
        myrequest = urllib.request.Request(url)
        return urllib.request.urlopen(myrequest)

    def process_granule(self, url: str, func: Callable, *args, **kwargs):
        """
        Perform an action on the contents of a granule while storing them in a memory file in RAM.

        :param url: url of granule's .h5 archive
        :param func: Method which takes an h5py File object as its first argument and performs the action.
        :param args: Passed to func.
        :param kwargs: Passed to func.
        :return: Result of func([.h5 archive at url], *args, **kwargs), or any caught exception.
        """
        response = self._request_raw_data(url)
        response.begin()
        with BytesIO() as memfile:
            try:
                while True:
                    chunk = response.read()
                    if chunk:
                        memfile.write(chunk)
                    else:
                        break
                memh5 = h5py.File(memfile, 'r')
                return func(memh5, *args, **kwargs)
            except Exception as e:
                print(f"An Exception of type {type(e)} caused failed download from {url}")
                return e
