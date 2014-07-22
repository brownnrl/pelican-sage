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

class ResultTypes:
    Image, Stream, Error = range(3)
    ALL_STR = ('image', 'stream', 'error')
    ALL_NUM = [x for x in range(3)]

Base = declarative_base()

class BaseMixin(object):
    @property
    def data(self):
        return self

class EvaluationType(Base, BaseMixin):
    __tablename__ = 'EvaluationType'
    ALL_NUM = (1,2,3)
    STATIC, DYNAMIC, CLIENT = ALL_NUM
    ALL_STR = ['static', 'dynamic', 'client']

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

class Src(Base, BaseMixin):
    __tablename__ = 'Src'
    id = Column(Integer, primary_key=True)
    src = Column(String, unique=True)
    
    code_blocks = relationship('CodeBlock', backref='Src')

class CodeBlock(Base, BaseMixin):
    __tablename__ = 'CodeBlock'
    id = Column(Integer, primary_key=True)
    user_id = Column(String, unique=True)
    src_id = Column(Integer, ForeignKey('Src.id'))
    order = Column(Integer)
    content = Column(String)
    eval_type_id = Column(Integer, ForeignKey('EvaluationType.id'),default=1)
    last_evaluated = Column(DateTime)

    stream_results = relationship('StreamResult', backref='CodeBlock')
    file_results = relationship('FileResult', backref='CodeBlock')
    error_results = relationship('ErrorResult', backref='CodeBlock')

    @property
    def results(self):
        return sorted(self.stream_results + self.file_results + self.error_results,
                      key = lambda x : x.order)

class StreamResult(Base, BaseMixin):
    __tablename__ = 'StreamResult'
    id = Column(Integer, primary_key=True)
    result = Column(String, nullable=True)
    code_id = Column(Integer, ForeignKey('CodeBlock.id'), nullable=False)
    order = Column(Integer)

    type = ResultTypes.Stream

class FileResult(Base, BaseMixin):
    __tablename__ = 'FileResult'
    id = Column(Integer, primary_key=True)
    file_location = Column(String)
    file_name = Column(String)
    order = Column(Integer)
    code_id = Column(Integer, ForeignKey('CodeBlock.id'), nullable=False)
    type = ResultTypes.Image

class ErrorResult(Base, BaseMixin):
    __tablename__ = 'ErrorResult'
    id = Column(Integer, primary_key=True)
    order = Column(Integer)
    ename = Column(String)
    evalue = Column(String)
    traceback = Column(String)
    code_id = Column(Integer, ForeignKey('CodeBlock.id'), nullable=False)
    type = ResultTypes.Error

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

        if 'CodeBlock' in insp.get_table_names():
            return

        Base.metadata.create_all(self._engine)

        self._session.add(EvaluationType(name='STATIC'))
        self._session.add(EvaluationType(name='DYNAMIC'))
        self._session.add(EvaluationType(name='CLIENT'))
        
        self._session.commit()

    def create_code(self, user_id=None, code=None, src=None, order=None):
        # check for an exisiting user id

        if user_id is not None:
            fetch = self._session.query(CodeBlock).filter_by(user_id=user_id).first()

            if fetch is not None:
                return fetch 

        src_obj = self._session.query(Src).filter_by(src=src).first()

        if src_obj is None:
            # Add the src object
            src_obj = Src(src=src)
            self._session.add(Src(src=src))
            self._session.commit()
            src_obj = self._session.query(Src).filter_by(src=src).first()

        fetch = self._session.query(CodeBlock).filter_by(src_id=src_obj.id,
                                                         order=order).first()

        if fetch is None:
            fetch = CodeBlock(src_id=src_obj.id,
                              content=code,
                              user_id=user_id,
                              order=order)
        elif fetch.content != code:
            fetch.content=code
            fetch.last_evaluated = None

        self._session.add(fetch)
        self._session.commit()
        self._session.refresh(fetch)

        return fetch

    def timestamp_code(self, code_id, timestamp=None):

        fetch = self._session.query(CodeBlock).filter_by(id=code_id).one()

        fetch.last_evaluated = self.io.datetime.now() if timestamp is None else timestamp

        self._session.add(fetch)

        self._session.commit()

    def get_code(self, code_id=None, user_id=None):

        if code_id is None and user_id is None:
            raise TypeError("Must provide either code_id or user_id")
        
        if user_id:
            return self._session.query(CodeBlock).filter_by(user_id=user_id).first()

        return self._session.query(CodeBlock).filter_by(id=code_id).first()


    def create_result(self, code_id, result_text, order=None):

        result = StreamResult(result=result_text, code_id=code_id, order=order)

        self._session.add(result)
        self._session.commit()

        return result

    def get_results(self, code_id):

        return self._session.query(CodeBlock).filter_by(id=code_id).stream_results

    def create_file(self, code_id, url, file_name, order=None):

        file_location = None

        if self._base_path is not None:
            file_location_path = self.io.join(self._base_path, str(code_id))
            self.io.create_directory_tree(file_location_path)
            file_location = self.io.join(file_location_path, file_name)

        if file_location is not None:
            self.io.download_file(url, file_location)

        file_result = FileResult(code_id=code_id,
                                 file_location=file_location if file_location else file_name,
                                 file_name=file_name,
				 order=order)

        self._session.add(file_result)

        self._session.commit()

        return file_result

    def get_files(self, code_id):

        return self._session.query(CodeBlock).filter_by(id=code_id).one().file_results


    def create_error(self, code_id, ename, evalue, traceback, order=None):
        error_result = ErrorResult(code_id=code_id,
                                   ename=ename,
                                   evalue=evalue,
                                   traceback=traceback,
                                   order=order)
        self._session.add(error_result)
        self._session.commit()
