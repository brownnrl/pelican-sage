import sqlite3
import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.types import DateTime
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.reflection import Inspector

from .pelicansageio import pelicansageio

class AlreadyExistsException(Exception):
    pass


Base = declarative_base()

class EvaluationType(Base):
    __tablename__ = 'EvaluationType'
    STATIC, DYNAMIC, CLIENT = (1,2,3)

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

class CodeBlock(Base):
    __tablename__ = 'CodeBlock'
    id = Column(Integer, primary_key=True)
    user_id = Column(String, unique=True)
    content = Column(String, unique=True)
    eval_type_id = Column(Integer, ForeignKey('EvaluationType.id'),default=1)
    last_evaluated = Column(DateTime)

    stream_results = relationship('StreamResult', backref='CodeBlock')
    file_results = relationship('FileResult', backref='CodeBlock')
    error_results = relationship('ErrorResult', backref='CodeBlock')

class StreamResult(Base):
    __tablename__ = 'StreamResult'
    id = Column(Integer, primary_key=True)
    result = Column(String, nullable=True)
    code_id = Column(Integer, ForeignKey('CodeBlock.id'), nullable=False)
    order = Column(Integer)

class FileResult(Base):
    __tablename__ = 'FileResult'
    columns = ('id', 'location', 'code_id')
    id = Column(Integer, primary_key=True)
    file_location = Column(String)
    order = Column(Integer)
    code_id = Column(Integer, ForeignKey('CodeBlock.id'), nullable=False)

class ErrorResult(Base):
    __tablename__ = 'ErrorResult'
    columns = ('id', 'ename', 'evalue', 'traceback', 'code_id')
    id = Column(Integer, primary_key=True)
    order = Column(Integer)
    ename = Column(String)
    evalue = Column(String)
    traceback = Column(String)
    code_id = Column(Integer, ForeignKey('CodeBlock.id'), nullable=False)

class FileManager(object):

    def __init__(self, location=None, base_path=None, db_name=None, io=None):

        self.io = pelicansageio if io is None else io

        # Throw away results after each computation of pelican pages
        if location and location != ':memory:':
            self.location = self.io.join(location, 'content.db' if db_name is None else db_name)
            self.io.create_directory_tree(location)
        else:
            self.location = ':memory:'

        self._engine = sqlalchemy.create_engine('sqlite:///' + self.location)

        self._session = sessionmaker(bind=self._engine)()

        self._base_path = base_path

        self._create_tables()

    def _create_tables(self):

        insp = Inspector.from_engine(self._engine)

        if 'CodeBlocks' in insp.get_table_names():
            return

        Base.metadata.create_all(self._engine)

        #hmm... i dunno...
        self._session.add(EvaluationType(name='STATIC'))
        self._session.add(EvaluationType(name='DYNAMIC'))
        self._session.add(EvaluationType(name='CLIENT'))
        
        self._session.commit()

    def create_code(self, user_id=None, code=None):
        # check for an existing user id

        if code is not None:
            fetch = self._session.query(CodeBlock).filter_by(content=code).first()

            if fetch is not None:
                return fetch 

        if user_id is not None:
            fetch = self._session.query(CodeBlock).filter_by(content=code).first()

            if fetch is not None:
                return fetch 

        code_block = CodeBlock(user_id=user_id, content=code)
        
        self._session.add(code_block)

        self._session.commit()

        return code_block

    def get_code(self, code_id=None, user_id=None):

        if code_id is None and user_id is None:
            raise TypeError("Must provide either code_id or user_id")
        
        if user_id:
            return self._session.query(CodeBlock).filter_by(user_id=user_id).first()

        return self._session.query(CodeBlock).filter_by(id=code_id).first()


    def create_result(self, code_id, result_text):

        result = StreamResult(result=result_text, code_id=code_id)

        self._session.add(result)
        self._session.commit()

        return result

    def get_results(self, code_id):

        return self._session.query(CodeBlock).filter_by(id=code_id).stream_results

    def create_file(self, code_id, url, file_name):

        file_location = None

        if self._base_path is not None:
            file_location_path = self.io.join(self._base_path, str(code_id))
            self.io.create_directory_tree(file_location_path)
            file_location = self.io.join(file_location_path, file_name)

        if file_location is not None:
            self.io.download_file(url, file_location)

        file_result = FileResult(code_id=code_id,
                                 file_location=file_location if file_location else file_name)

        self._session.add(file_result)

        self._session.commit()

        return file_result

    def get_files(self, code_id):

        return self._session.query(CodeBlock).filter_by(id=code_id).one().file_results

