import unittest

from pelicansage.managefiles import FileManager, AlreadyExistsException

from datetime import datetime

# Test parameter
url = 'http://www.whereverasdasdsadsadasd.com'

class TestFileManager(unittest.TestCase):

    def test_create_code_content(self):
        manager = FileManager()
        code = "xx"
        code_obj = manager.create_code(user_id='stuff', code=code, src='A.rst', order=1)

        code_obj_2 = manager.get_code(user_id='stuff')
        self.assertEquals(code_obj_2.content, code)

        code_obj_2 = manager.get_code(code_id=code_obj.id)
        self.assertEquals(code_obj_2.content, code)

        # Code with the same content has the same returned value
        self.assertEquals(manager.create_code(code=code, src='A.rst', order=1).id, code_obj.id)

        self.assertEquals(manager.get_code(code_id=444), None)

        self.assertEquals(manager.get_code(user_id='DNE'), None)

    def test_create_code(self):
        manager = FileManager()
        self.assertTrue(manager.create_code() is not None,
                "Create code did not return a valid primary key.")
        self.assertTrue(manager.create_code('myuniqueid') is not None)
        try:
            manager.create_code('myuniqueid')
        except AlreadyExistsException:
            pass

    def test_create_file(self):
        manager = FileManager()
        code_obj = manager.create_code()

        file_obj = manager.create_file(code_obj.id, url, 'xxx.png')
        verify = [(file_obj.id, 'xxx.png')]
        self.assertEquals([(fileobj.id, fileobj.file_location) 
                            for fileobj in manager.get_files(code_obj.id)],
                          verify)

        manager.create_code()

        file_obj_2 = manager.create_file(code_obj.id, url, 'xxx2.png')

        verify.append((file_obj_2.id, 'xxx2.png'))
        self.assertEquals([(fileobj.id, fileobj.file_location) 
                            for fileobj in manager.get_files(code_obj.id)],
                          verify)

    def test_create_result(self):
        manager = FileManager()
        code_obj = manager.create_code()

        file_obj = manager.create_file(code_obj.id, url, 'xxx.png')
        verify = [(file_obj.id, 'xxx.png')]
        self.assertEquals([(x.id, x.file_location) for x in manager.get_files(code_obj.id)],
                          verify)

        manager.create_code()

        file_obj_2 = manager.create_file(code_obj.id, url, 'xxx2.png')

        verify.append((file_obj_2.id, 'xxx2.png'))
        self.assertEquals([(x.id, x.file_location) for x in manager.get_files(code_obj.id)],
                          verify)

    def test_timestamp(self):

        class dummy_io(object):
            class datetime_dummy(object):
                now = lambda self : datetime(year=2014,day=1,month=1)

            datetime = datetime_dummy()

        manager = FileManager(io=dummy_io())

        code_obj = manager.create_code()

        self.assertEquals(code_obj.last_evaluated, None)

        manager.timestamp_code(code_obj.id)

        code_obj = manager.get_code(code_id = code_obj.id)

        self.assertEquals(code_obj.last_evaluated, dummy_io().datetime.now())

if __name__ == '__main__':
    unittest.main()
