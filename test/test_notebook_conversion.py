import unittest
from datetime import datetime

from pelicansage.pelicansage import process_ipynb
from pelicansage.managefiles import FileManager

import os as pyos
from testutil import test_data


class TestNotebookConversion(unittest.TestCase):

    def test_nb3_format(self):
        class dummy_io(object):
            class datetime_dummy(object):
                def now(self):
                    return datetime(year=2014, day=1, month=1)

            def copy_file(self, *args):
                pass

            os = pyos

            datetime = datetime_dummy()

        manager = FileManager(io=dummy_io())
        nb3_path = test_data.filepath('notebooks/notebook_haskell_sample_nb3.ipynb')
        content_path = test_data.filepath('notebooks/')
        test_output_path = test_data.filepath('test_output')

        process_ipynb(manager, nb3_path, content_path, test_output_path)

        cbs = manager.get_all_codeblocks()
        for cb in cbs:
            for ident, order, result, mime in [(r.id, r.order, r.result, r.mimetype) for r in cb.stream_results]:
                if len(result) < 2000:
                    print(ident, order, mime, result)

    def test_nb4_format(self):
        class dummy_io(object):
            class datetime_dummy(object):
                def now(self):
                    return datetime(year=2014, day=1, month=1)

            def copy_file(self, *args):
                pass

            os = pyos

            datetime = datetime_dummy()

        manager = FileManager(io=dummy_io())
        nb3_path = test_data.filepath('notebooks/notebook_haskell_sample_nb4.ipynb')
        content_path = test_data.filepath('notebooks/')
        test_output_path = test_data.filepath('test_output')


        process_ipynb(manager, nb3_path, content_path, test_output_path)
