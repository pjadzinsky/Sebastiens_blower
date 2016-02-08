""" Commonly used network operations. """

from common.log import *
from common.utils import hash_utils
from common.utils import decorators
#from thirdparty.google.base import py_base
import requests
import tempfile
import uuid

MOUSERA_TMP_DOWNLOAD = '/tmp/mousera/download'


class DownloadError(Exception):

    """Exception raised for errors when requested thing can not be downloaded.
        msg  -- explanation of the error        
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return '<DownloadError: %s>' % (self.msg)



def make_parent_dirs(filename):  
    parent_dirs = os.path.dirname(filename)  
    if not os.path.exists(parent_dirs):  
        os.makedirs(parent_dirs)
    return 

def make_dirs_if_needed(path):  
    if not os.path.exists(path):  
        os.makedirs(path)
    return


def url_exists(url):
    exists = False
    # print 'testing url: %s' % (url)
    try:
        r = requests.get(url, stream=True, timeout=1.0)
        if r.ok:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    exists = True
                    break
    except requests.exceptions.Timeout:
        pass
    return exists


def download_url(url, filename):
    make_parent_dirs(filename)
    make_dirs_if_needed(MOUSERA_TMP_DOWNLOAD)
    handle, tmp_filename = tempfile.mkstemp(dir=MOUSERA_TMP_DOWNLOAD)
    exists = False
    r = requests.get(url, stream=True, timeout=5.0)
    print 'downloading %s -> %s' % (url, filename)

    if not r.ok:
        raise DownloadError('Download url failed: %s %s' % ( url, r))

    f = open(tmp_filename, 'wb')
    for chunk in r.iter_content(chunk_size=1024000):
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
            sys.stdout.write('.')
            sys.stdout.flush()
    f.close()
    sys.stdout.write('\n')
    os.rename(tmp_filename, filename)
    return


def fetch_tarball(url, md5):
    make_dirs_if_needed(MOUSERA_TMP_DOWNLOAD)
    handle, tmp_filename = tempfile.mkstemp(dir=MOUSERA_TMP_DOWNLOAD)
    download_url(url, tmp_filename)
    computed_md5 = hash_utils.md5sum(tmp_filename)
    CHECK_EQ(md5, computed_md5)
    return tmp_filename

import tarfile


@decorators.retry(3)
def fetch_and_unarchive(archive_url, md5):
    tarball_file = fetch_tarball(archive_url, md5)
    make_dirs_if_needed(MOUSERA_TMP_DOWNLOAD)
    tmp_dir = tempfile.mkdtemp(dir=MOUSERA_TMP_DOWNLOAD)
    tar = tarfile.open(tarball_file)
    tar.extractall(tmp_dir)
    tar.close()
    return tmp_dir

CACHED_MAC_ADDRESS = None

def get_mac_address():
    """ returns mac address as a large integer. """
    global CACHED_MAC_ADDRESS
    if not CACHED_MAC_ADDRESS:
        CACHED_MAC_ADDRESS = uuid.getnode()
    return CACHED_MAC_ADDRESS