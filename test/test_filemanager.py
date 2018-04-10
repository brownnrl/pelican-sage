import unittest

from pelicansage.managefiles import FileManager

from datetime import datetime

# Test parameter
url = 'http://www.whereverasdasdsadsadasd.com'
import os as pyos


class TestFileManager(unittest.TestCase):
    def test_create_code_content(self):
        manager = FileManager()
        code = "xx"
        code_obj = manager.create_code(user_id='stuff', code=code, src='A.rst', order=1)

        code_obj_2 = manager.get_code(src='A.rst', user_id='stuff')
        self.assertEqual(code_obj_2.content, code)

        code_obj_2 = manager.get_code(code_id=code_obj.id)
        self.assertEqual(code_obj_2.content, code)

        # Code with the same content has the same returned value
        self.assertEqual(manager.create_code(code=code, src='A.rst', order=1).id, code_obj.id)

        self.assertEqual(manager.get_code(code_id=444), None)

        self.assertEqual(manager.get_code(user_id='DNE'), None)

    def test_create_code(self):
        manager = FileManager()
        src = 'a.rst'
        order = 1
        code = 'xxx'
        code_obj1 = manager.create_code(code, src, order)
        self.assertTrue(code_obj1 is not None,
                        "Create code did not return a valid primary key.")
        self.assertTrue(manager.create_code(code, src, order + 1, 'myuniqueid') is not None)
        code_obj2 = manager.create_code(code, src, order + 2, 'myuniqueid')

        self.assertEqual(code_obj1.user_id, None)
        self.assertEqual(code_obj2.user_id, 'myuniqueid')
        self.assertEqual(code_obj2.order, order + 2)

    def test_create_file(self):
        manager = FileManager()
        src = 'a.rst'
        order = 1
        code = 'xxx'
        code_obj = manager.create_code(code, src, order)

        file_obj = manager.create_file(code_obj.id, url, 'xxx.png')
        verify = [(file_obj.id, 'xxx.png')]
        self.assertEqual([(fileobj.id, fileobj.file_name)
                           for fileobj in manager.get_files(code_obj.id)],
                          verify)

        manager.create_code(code, src, order + 1)

        file_obj_2 = manager.create_file(code_obj.id, url, 'xxx2.png')

        manager.commit()

        verify.append((file_obj_2.id, 'xxx2.png'))
        self.assertEqual([(fileobj.id, fileobj.file_name)
                           for fileobj in manager.get_files(code_obj.id)],
                          verify)

    def test_create_result(self):
        manager = FileManager()
        src = 'a.rst'
        order = 1
        code = 'xxx'
        code_obj = manager.create_code(code, src, order)

        result_obj = manager.create_result(code_obj.id, 'xxx', 1)
        verify = [(result_obj.id, 'xxx')]
        self.assertEqual([(x.id, x.data) for x in manager.get_results(code_obj.id)],
                          verify)

        code_obj_2 = manager.create_code(code, src, order + 1)

        result_obj_2 = manager.create_result(code_obj.id, 'yyy', 2)

        result_obj_3 = manager.create_result(code_obj_2.id, 'yyy', 1)

        manager.commit()

        verify.append((result_obj_2.id, 'yyy'))
        self.assertEqual([(x.id, x.data) for x in manager.get_results(code_obj.id)],
                          verify)

        # Now, we change the code block and assert that there are no results left
        # for the deleted blocks

        manager.create_code(code + 'some change', src, order)

        self.assertEqual(manager.get_results(code_obj_2.id), [])

    def test_reference(self):
        manager = FileManager()

        src1 = 'a.rst'
        src2 = 'b.rst'
        src3 = 'c.rst'

        src_ref_obj = manager.create_reference(src1, src2)

        src1_obj = manager.create_src(src1)
        src2_obj = manager.create_src(src2)

        manager.commit()

        self.assertEqual([srcref.src2.src for srcref in src1_obj.references],
                         [src2])

        self.assertEqual(src1_obj.references[0].src2.src, src2_obj.src)

        src_ref_obj = manager.create_reference(src1, src3)

        manager.commit()

        self.assertEqual(len(src1_obj.references), 2)

        self.assertEqual([srcref.src2.src for srcref in src1_obj.references],
                         [src2, src3])

    def test_timestamp(self):
        src = 'a.rst'
        order = 1
        code = 'xxx'

        class dummy_io(object):
            class datetime_dummy(object):
                def now(self):
                    return datetime(year=2014, day=1, month=1)

            os = pyos

            datetime = datetime_dummy()

        manager = FileManager(io=dummy_io())

        code_obj = manager.create_code(code, src, order)

        self.assertEqual(code_obj.last_evaluated, None)

        manager.timestamp_code(code_obj.id)

        code_obj = manager.get_code(code_id=code_obj.id)

        self.assertEqual(code_obj.last_evaluated, dummy_io().datetime.now())


if __name__ == '__main__':
    unittest.main()
