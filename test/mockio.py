
from pelicansage.pelicansageio import pelicansageio

class MockIO(object):

    def download_file(self, *args):
        pass

    def create_directory_tree(self, *args):

    def Request(self, *args, **kwargs):
        pass

    def get_response(self, repsonse):
        pass
