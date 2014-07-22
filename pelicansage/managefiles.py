import sqlite3
import os.path

class AlreadyExistsException(Exception):
    pass

class FileManager(object):

    def __init__(self, location=None):
        self.location = ':memory:' if location is None else location

        create_tables = self.location == ':memory:' or os.path.isfile(self.location)

        self._conn = sqlite3.connect(self.location)

        if create_tables:
            self._create_tables()

    def _create_tables(self):
        code = """
        CREATE TABLE CODEBLOCKS(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NULL,
            content TEXT NULL
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

        cursor.execute("SELECT * FROM CODEBLOCKS WHERE user_id=?", (user_id,))

        if cursor.rowcount > 0:
            raise AlreadyExistsException(user_id)

        cursor.execute("INSERT INTO CODEBLOCKS (user_id, content) VALUES (?,?)", (user_id, code))
        last_id = cursor.lastrowid

        self._conn.commit()
        return last_id

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

