import sqlite3

from .pelicansageio import pelicansageio

class AlreadyExistsException(Exception):
    pass


class ORM(object):

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k) and v is not None:
                setattr(self, k, v)

class Table(ORM):
    name = ''
    columns = tuple()

    def __init__(self, **kwargs):
        for column in self.columns:
            setattr(self, column, '')
        self.update(**kwargs)

    def select(self, **kwargs):
        items = kwargs.items()
        where  = "WHERE " + ''.join(["%s = ? " % (k,) for k, _ in items])
        select = "SELECT * FROM %s%s" % (self.name, where)


    @classmethod
    def create(klass, *args):
        zipped = dict(zip(klass.columns, args))
        return klass(**zipped)

class EvaluationType(Table):
    STATIC, DYNAMIC, CLIENT = (1,2,3)
    create_sql = """
                CREATE TABLE EVALUATION_TYPES(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE
                );

                INSERT INTO EVALUATION_TYPES (name) VALUES ('STATIC');
                INSERT INTO EVALUATION_TYPES (name) VALUES ('DYNAMIC');
                INSERT INTO EVALUATION_TYPES (name) VALUES ('CLIENT');
                """

class CodeBlock(Table):
    name = 'CODEBLOCKS'
    columns = ('id', 'user_id', 'content', 'eval_type_id')
    create_sql = """
                CREATE TABLE CODEBLOCKS(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NULL,
                    content TEXT UNIQUE NULL,
                    eval_type_id INTEGER DEFAULT 1,
                    last_evaluated timestamp,
                    FOREIGN KEY(eval_type_id) REFERENCES EVALUATION_TYPES(id)
                );
                """
                


class StreamResult(Table):
    name = 'STREAM_RESULTS'
    columns = ('id', 'result', 'code_id')
    create_sql = """
                 CREATE TABLE STREAM_RESULTS(
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     result TEXT NULL,
                     code_id INTEGER,
                     FOREIGN KEY(code_id) REFERENCES CODEBLOCKS(id)
                 );
                 """

class FileResult(Table):
    name = 'FILE_RESULTS'
    columns = ('id', 'location', 'code_id')
    create_sql = """
                 CREATE TABLE FILE_RESULT(
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     file_location TEXT,
                     code_id INTEGER,
                     FOREIGN KEY(code_id) REFERENCES CODEBLOCKS(id)
                 );
                 """

class ErrorResult(Table):
    name = 'ERROR_RESULTS'
    columns = ('id', 'ename', 'evalue', 'traceback', 'code_id')
    create_sql = """
                 CREATE TABLE ERROR_RESULTS(
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     ename TEXT,
                     evalue TEXT,
                     traceback TEXT,
                     code_id INTEGER,
                     FOREIGN KEY(code_id) REFERENCES CODEBLOCKS(id)
                 );
                 """

tables = (CodeBlock, StreamResult, FileResult, ErrorResult)

class FileManager(object):

    def __init__(self, location=None, base_path=None, db_name=None, io=None):

        self.io = pelicansageio if io is None else io

        # Throw away results after each computation of pelican pages
        if location and location != ':memory:':
            self.location = self.io.join(location, 'content.db' if db_name is None else db_name)
            self.io.create_directory_tree(location)
        else:
            self.location = ':memory:'

        self._conn = sqlite3.connect(self.location, detect_types=sqlite3.PARSE_DECLTYPES)

        self._base_path = base_path

        self._create_tables()

    def _create_tables(self):

        global tables

        code = ''.join([table.create_sql for table in tables])

        cursor = self._conn.cursor()
    
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='CODEBLOCKS'")

        if cursor.fetchone() is not None:
            return

        for c in code.split(';'):
            cursor.execute(c)

        self._conn.commit()

    def create_code(self, user_id=None, code=None):
        # check for an existing user id
        cursor = self._conn.cursor()

        if code is not None:
            cursor.execute("SELECT * FROM CODEBLOCKS WHERE content=?", (code,))

            fetch = cursor.fetchone()

            if fetch is not None:
                return CodeBlock.create(*fetch)

        if user_id is not None:
            cursor.execute("SELECT * FROM CODEBLOCKS WHERE user_id = ?", (user_id,))

            if cursor.fetchone() is not None:
                raise AlreadyExistsException(user_id)

        cursor.execute("INSERT INTO CODEBLOCKS (user_id, content) VALUES (?,?)", (user_id, code))
        last_id = cursor.lastrowid

        self._conn.commit()

        cursor.execute("SELECT * FROM CODEBLOCKS WHERE id=?", (last_id,))
        return CodeBlock.create(*cursor.fetchone())

    def get_code(self, code_id=None, user_id=None):

        if code_id is None and user_id is None:
            raise TypeError("Must provide either code_id or user_id")
        
        cursor = self._conn.cursor()
        ident = ('id', code_id) if user_id is None else ('user_id', user_id)

        cursor.execute("SELECT * FROM CODEBLOCKS WHERE %s=?" % (ident[0],), (ident[1],))

        result = cursor.fetchone() 

        return CodeBlock.create(*result) if result else None

    def create_result(self, code_id, result_text):
        cursor = self._conn.cursor()

        cursor.execute("INSERT INTO STREAM_RESULTS (result, code_id) VALUES (?, ?)",
                        (result_text, code_id))

        last_id = cursor.lastrowid

        self._conn.commit()

        cursor.execute("SELECT * FROM STREAM_RESULTS WHERE id=?", (last_id,))
        return StreamResult.create(*cursor.fetchone()) 

    def get_results(self, code_id):
        cursor = self._conn.cursor()

        cursor.execute("SELECT * FROM STREAM_RESULTS WHERE code_id=?", (code_id,))

        return [StreamResult.create(*row) for row in cursor]

    def create_file(self, code_id, url, file_name):
        cursor = self._conn.cursor()

        file_location = None

        if self._base_path is not None:
            file_location_path = self.io.join(self._base_path, str(code_id))
            self.io.create_directory_tree(file_location_path)
            file_location = self.io.join(file_location_path, file_name)


        cursor.execute("INSERT INTO FILE_RESULT (file_location, code_id) VALUES (?, ?)",
                       (file_name, code_id))

        last_id = cursor.lastrowid
        self._conn.commit()

        if file_location is not None:
            self.io.download_file(url, file_location)
        
        cursor.execute("SELECT * FROM FILE_RESULT WHERE id=?", (last_id,))

        fetch = cursor.fetchone()

        return FileResult.create(*fetch)

    def get_files(self, code_id):
        cursor = self._conn.cursor()

        cursor.execute("SELECT * FROM FILE_RESULT WHERE code_id = ?", (code_id,))

        return [FileResult.create(*row) for row in cursor]

