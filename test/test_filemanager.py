import unittest

from pelicansage.managefiles import FileManager, AlreadyExistsException

class TestFileManager(unittest.TestCase):

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
        code_id = manager.create_code()

        file_id = manager.create_file(code_id, 'xxx.png')
        verify = [(file_id, 'xxx.png')]
        self.assertEquals(manager.get_files(code_id), verify)

        manager.create_code()

        file_id_2 = manager.create_file(code_id, 'xxx2.png')

        verify.append((file_id_2, 'xxx2.png'))
        self.assertEquals(manager.get_files(code_id), verify)




if __name__ == '__main__':
    unittest.main()
