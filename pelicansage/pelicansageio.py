
# Independent IO access so it can be mocked to create tests
# and uniformly work on python 2/3 for the purposes of this
# application


import os, sys
import errno

import requests

import urllib, shutil

try:
    import urllib.request
    Request = urllib.request.Request
    def _grab_file(url, file_name):
        with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
           shutil.copyfileobj(response, out_file)
    def get_response(req):
        return urllib.request.urlopen(req).read().decode('utf8')
except ImportError:
    _grab_file = urllib.urlretrieve

    import urllib2
    Request = urllib2.Request

    def get_response(req):
        return urllib2.urlopen(req).read()

def download_file(url, file_name):
    return _grab_file(url, file_name)

if sys.version_info.major == 3:
    to_bytes = lambda x: bytes(x, 'utf-8')
else:
    to_bytes = str

# Accepted answer,
# http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python 
def create_directory_tree(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

join = os.path.join

class Globals(object):
    def __getattr__(self, attr):
        return globals()[attr]

pelicansageio = Globals()
