import py.path
import os.path as osp


def no_endings(x):
    for end in ('\r', '\n'):
        x = x.replace(end, "")
    return x


class TestData:
    def __init__(self, directory_path):
        self.data_directory = py.path.local(directory_path)

    def filepath(self, relative_path, *args, test_path=True):
        resultpath = self.data_directory.join(relative_path)
        if args:
            resultpath = resultpath.join(*args)
        if test_path:
            assert resultpath.check()
        return str(resultpath)

    def read_from_path(self, path, *args):
        with open(self.filepath(path, *args), 'r') as fp:
            return fp.read()


test_data = TestData(osp.join(osp.dirname(__file__), 'testdata'))
