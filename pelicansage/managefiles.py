import sqlite3
import os.path

class AlreadyExistsException(Exception):
    pass

class FileManager(object):

    def __init__(self, location=None):
        # Throw away results after each computation of pelican pages
        self.location = ':memory:' if location is None else location


        # Create the tables if we don't already have an existing file.
        # If we have an existing file we assume that the tables have already
        # been created.
        create_tables = self.location == ':memory:' or not os.path.isfile(self.location)

        self._conn = sqlite3.connect(self.location)

        if create_tables:
            self._create_tables()

    def _create_tables(self):
        code = """
        CREATE TABLE CODEBLOCKS(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NULL,
            content TEXT UNIQUE NULL
        );

        CREATE TABLE STREAM_RESULTS(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            result TEXT NULL,
            code_id INTEGER,
            FOREIGN KEY(code_id) REFERENCES CODEBLOCKS(id)
        );

        CREATE TABLE FILE_RESULT(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_location TEXT,
            code_id INTEGER,
            FOREIGN KEY(code_id) REFERENCES CODEBLOCKS(id)
        );
        """

        cursor = self._conn.cursor()

        for c in code.split(';'):
            cursor.execute(c)

        self._conn.commit()

    def create_code(self, user_id=None, code=None):
        # check for an existing user id
        cursor = self._conn.cursor()


        if code is not None:
            cursor.execute("SELECT id, content FROM CODEBLOCKS WHERE content=?", (code,))

            fetch = cursor.fetchone()

            if fetch is not None:
                return fetch[0]

        if user_id is not None:
            cursor.execute("SELECT * FROM CODEBLOCKS WHERE user_id = ?", (user_id,))

            if cursor.fetchone() is not None:
                raise AlreadyExistsException(user_id)

        cursor.execute("INSERT INTO CODEBLOCKS (user_id, content) VALUES (?,?)", (user_id, code))
        last_id = cursor.lastrowid

        self._conn.commit()
        return last_id

    def get_code_content(self, code_id=None, user_id=None):

        if code_id is None and user_id is None:
            raise TypeError("Must provide either code_id or user_id")
        
        cursor = self._conn.cursor()
        ident = ('id', code_id) if user_id is None else ('user_id', user_id)

        cursor.execute("SELECT content FROM CODEBLOCKS WHERE %s=?" % (ident[0],), (ident[1],))

        result = cursor.fetchone() 

        return result[0] if result else None

    def create_result(self, code_id, result_text):
        cursor = self._conn.cursor()

        cursor.execute("INSERT INTO STREAM_RESULTS (result, code_id) VALUES (?, ?)",
                        (result_text, code_id))

        last_id = cursor.lastrowid

        self._conn.commit()
        return last_id

    def get_results(self, code_id):
        cursor = self._conn.cursor()

        cursor.execute("SELECT id, result FROM STREAM_RESULTS WHERE code_id=?", (code_id,))

        return [row for row in cursor]

    def create_file(self, code_id, file_location):
        cursor = self._conn.cursor()

        cursor.execute("INSERT INTO FILE_RESULT (file_location, code_id) VALUES (?, ?)",
                       (file_location, code_id))

        last_id = cursor.lastrowid
        self._conn.commit()

        return last_id

    def get_files(self, code_id):
        cursor = self._conn.cursor()

        cursor.execute("SELECT id, file_location FROM FILE_RESULT WHERE code_id = ?", (code_id,))

        return [row for row in cursor]

