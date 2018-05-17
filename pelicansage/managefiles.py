from sqlite3 import IntegrityError

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, Enum
from sqlalchemy.types import DateTime
from sqlalchemy.orm import sessionmaker, relationship, mapper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.reflection import Inspector

from .pelicansageio import pelicansageio

import zlib
import base64
import sys

from uuid import uuid4

class ResultTypes:
    Image, Stream, Error = range(3)
    ALL_STR = ('image', 'stream', 'error')
    ALL_NUM = [x for x in range(3)]

Base = declarative_base()

MimeType = Enum('text/plain',
                'text/html',
                'text/image-filename', 
                'text/x-python-traceback',
                'image/png')

LanguagesStrEnum = ('python',
                   'sage',
                   'haskell',
                   'scala',
                   'java',
                   'groovy',
                   'kotlin',
                   'clojure',
                   'r',
                   'octave',
                   'maxima',
                   'gap',
                   'gp')

Platforms = Enum('sage', 'ipython', 'ihaskell', 'ipynb')

FileTypes = Enum('rst', 'ipynb', 'json')

Languages = Enum(*LanguagesStrEnum)

class BaseMixin(object):
    @property
    def data(self):
        return self

class EvaluationType(Base, BaseMixin):
    __tablename__ = 'EvaluationType'
    ALL_NUM = (1, 2, 3)
    STATIC, DYNAMIC, CLIENT = ALL_NUM
    ALL_STR = ['static', 'dynamic', 'client']

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

class SrcReference(Base):
    __tablename__ = 'SrcReference'
    src_id1 = Column(Integer, ForeignKey('DataSrc.id'), primary_key=True)
    src_id2 = Column(Integer, ForeignKey('DataSrc.id'), primary_key=True)

    src1 = relationship('DataSrc', primaryjoin='(DataSrc.id == SrcReference.src_id1)')
    src2 = relationship('DataSrc', primaryjoin='(DataSrc.id == SrcReference.src_id2)')

class DataSrc(Base, BaseMixin):
    __tablename__ = 'DataSrc'
    id = Column(Integer, primary_key=True)
    src = Column(String, unique=True)
    permalink = Column(String)
    filetype = Column(FileTypes, default='rst')
    
    code_blocks = relationship('CodeBlock', backref='DataSrc',
                                cascade='save-update, merge, delete')

    references = relationship('SrcReference', 
                              primaryjoin=(id == SrcReference.src_id1))

class CodeBlock(Base, BaseMixin):
    __tablename__ = 'CodeBlock'
    id = Column(Integer, primary_key=True)
    user_id = Column(String)
    src_id = Column(Integer, ForeignKey('DataSrc.id'))
    order = Column(Integer)
    content = Column(String)
    eval_type_id = Column(Integer, ForeignKey('EvaluationType.id'),default=1)
    last_evaluated = Column(DateTime)
    permalink = Column(String)
    language = Column(Languages)
    platform = Column(Platforms)

    src = relationship('DataSrc', backref='DataSrc')
    stream_results = relationship('StreamResult', backref='CodeBlock',
                                   cascade='save-update, merge, delete')
    file_results = relationship('FileResult', backref='CodeBlock',
                                   cascade='save-update, merge, delete')
    error_results = relationship('ErrorResult', backref='CodeBlock',
                                   cascade='save-update, merge, delete')

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
    mimetype = Column(MimeType)

    @property
    def data(self):
        return self.result

    type = ResultTypes.Stream

class FileResult(Base, BaseMixin):
    __tablename__ = 'FileResult'
    id = Column(Integer, primary_key=True)
    file_name = Column(String)
    order = Column(Integer)
    code_id = Column(Integer, ForeignKey('CodeBlock.id'), nullable=False)
    mimetype = Column(MimeType)
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
    mimetype = 'text/x-python-traceback'

class FileManager(object):

    def __init__(self, location=None, base_path=None, db_name=None, io=None, echo_sql=False):
        self.io = pelicansageio if io is None else io

        # Throw away results after each computation of pelican pages
        if location and location != ':memory:':
            self.location = self.io.join(location, 'content.db' if db_name is None else db_name)
            self.io.create_directory_tree(location)
        else:
            self.location = ':memory:'

        self._engine = sqlalchemy.create_engine('sqlite:///' + self.location, echo=echo_sql)

        self._session = sessionmaker(bind=self._engine)()
        _SESSION = self._session

        self._base_path = base_path

        self._create_tables()

        self._current_evaluations = set() 

    def _create_tables(self):

        insp = Inspector.from_engine(self._engine)

        if 'CodeBlock' in insp.get_table_names():
            return

        Base.metadata.create_all(self._engine)

        self._session.add(EvaluationType(name='STATIC'))
        self._session.add(EvaluationType(name='DYNAMIC'))
        self._session.add(EvaluationType(name='CLIENT'))
        
        self._session.commit()

    def commit(self):
        self._session.commit()
    
    def get_all_codeblocks(self):
        """
        Returns all code blocks.
        """
        blks = self._session.query(CodeBlock).all()

        return blks

    def get_unevaluated_codeblocks(self):
        """
        Returns a list of lists of code blocks which require evalution.

        Each sub-list represents invidual blocks that should be evaluated
        in the same namespace sequentially with results collected and
        correlated to each block.  

        Each sublist likely represents the blocks contained within or 
        referenced by a code block.

        However, each sub-list can be evaluated asynchronously.
        """
        srcs = self._session.query(DataSrc).join(CodeBlock).filter(CodeBlock.last_evaluated == None,
                                                                   CodeBlock.platform != 'ipynb',
                                                                   DataSrc.filetype != 'ipynb').all()

        blocks = [src.code_blocks for src in srcs]

        def flatten(l):
            return [item for sublist in l for item in sublist]

        refs = flatten([src.references for src in srcs])

        return blocks, refs


    def create_reference(self, src1, src2):

        src1_obj = self.create_src(src1)
        src2_obj = self.create_src(src2)

        src_ref_obj = self._session.query(SrcReference)\
                            .filter(SrcReference.src_id1==src1_obj.id,
                                    SrcReference.src_id2==src2_obj.id)\
                            .first()

        if src_ref_obj is None:
            src_ref_obj = SrcReference(src_id1=src1_obj.id, src_id2=src2_obj.id)
            self._session.add(src_ref_obj)
            self._session.flush()#self._session.commit()

        return src_ref_obj

    def create_src(self, src):
        src_obj = self._session.query(DataSrc).filter_by(src=src).first()

        ext = self.io.os.path.splitext(src)[1][1:]

        if src_obj is None:
            # Add the src object
            src_obj = DataSrc(src=src, filetype=ext)
            self._session.add(src_obj)
            self._session.flush()#self._session.commit()
            src_obj = self._session.query(DataSrc).filter_by(src=src).first()

        return src_obj

    def compute_permalink(self, src):
        src_obj = self.create_src(src)

        blocks = src_obj.code_blocks

        if len(blocks) == 0:
            return

        code = ''
        code += ("\npretty_print(html('<br/><hr/><br/>'))\n#" + '-'*40 + "\n").join([block.content for block in blocks])

        def gen_permalink(content):

            if sys.version_info[0] > 2:
                content = bytes(content, 'UTF-8')

            permalink = base64.urlsafe_b64encode(zlib.compress(content))

            if sys.version_info[0] > 2:
                permalink = permalink.decode('UTF-8')

            return permalink

        src_obj.permalink = gen_permalink(code)

        self._session.add(src_obj)

        for block in blocks:
            block.permalink = gen_permalink(block.content)
            self._session.add(block)

        self._session.flush()

    def create_code(self, code, src, order, user_id=None, language='sage', platform='sage'):
        # check for an exisiting user id

        if user_id is not None:
            fetch = self._session.query(CodeBlock)\
                                 .join(DataSrc)\
                                 .filter(DataSrc.src == src, CodeBlock.user_id == user_id)\
                                 .first()

            if fetch is not None:
                if fetch.order != order:
                    # Resolve by removing the other tag.
                    # So 'last tag remaining' wins.
                    fetch.user_id = None
                    self._session.add(fetch)
                    self._session.flush()#self._session.commit()

        src_obj = self.create_src(src)

        fetch = self._session.query(CodeBlock).filter_by(src_id=src_obj.id,
                                                         order=order).first()

        if fetch is None:
            fetch = CodeBlock(src_id=src_obj.id,
                              content=code,
                              language=language,
                              platform=platform,
                              user_id=user_id,
                              order=order)
        elif fetch.content != code:

            # We will need to regenerate results for this source file
            code_blocks_in_src = self._session.query(CodeBlock).filter(CodeBlock.src_id == fetch.src_id).all()

            for code_obj in code_blocks_in_src:

                code_id = code_obj.id

                if self._base_path is not None:
                    file_location_path = self.io.join(self._base_path, str(code_id))
                    self.io.delete_directory(file_location_path)

                for table in (StreamResult, ErrorResult, FileResult):
                    query = self._session.query(table).filter(table.code_id==code_id)

                    query.delete()

            # get all id's for the source

            # We remove all code blocks for that source
            self._session.query(CodeBlock).filter(CodeBlock.src_id==src_obj.id,
                                                  CodeBlock.order > fetch.order).delete()


            self._session.commit()
            
            fetch.content=code
            fetch.user_id = user_id
            fetch.last_evaluated = None

        self._session.add(fetch)
        self._session.flush()
        self._session.commit()
        self._session.refresh(fetch)

        return fetch

    def timestamp_code(self, code_id, timestamp=None):

        fetch = self._session.query(CodeBlock).filter_by(id=code_id).one()

        fetch.last_evaluated = self.io.datetime.now() if timestamp is None else timestamp

        self._session.add(fetch)

        self._session.flush()#self._session.commit()

    def get_code(self, code_id=None, user_id=None, src=None):

        if src is not None and user_id is not None:
        
            src_obj = self.create_src(src)

            fetch = self._session.query(CodeBlock).filter_by(user_id=user_id,
                                                             src_id=src_obj.id).first()

            return fetch

        return self._session.query(CodeBlock).filter_by(id=code_id).first()

    def mark_evaluated(self, code_obj):

        self._current_evalautions.add((code_obj.src.id, code_obj.id))
        code_obj.last_evaluated = datetime.now()

        self._session.add(code_obj)
        self._session.flush()#self._session.commit()

    def get_code_block_chain(self, code_obj):

        if (code_obj.src.id, code_obj.id) in self._current_evaluations:
            return []

        code_objects = self._session.query(CodeBlock).filter_by(src_id=code_obj.src.id)

    def create_result(self, code_id, result_text, order=None, mimetype='text/plain'):

        result = StreamResult(result=result_text, code_id=code_id, order=order, mimetype=mimetype)

        self._session.add(result)
        self._session.flush()#self._session.commit()

        return result

    def get_results(self, code_id):
        code_obj = self._session.query(CodeBlock).filter_by(id=code_id).first()
        if code_obj is None:
            return []

        return code_obj.stream_results

    def save_file(self, code_id, raw, file_name, order=None, mimetype=None):
        file_location = None

        if self._base_path is not None:
            file_location_path = self.io.join(self._base_path, str(code_id))
            self.io.create_directory_tree(file_location_path)
            file_location = self.io.join(file_location_path, file_name)

        self.io.save_data_to_file(raw, file_location)

        file_result = FileResult(code_id=code_id,
                                 file_name=file_name,
                                 order=order,
                                 mimetype=mimetype)

        self._session.add(file_result)

        self._session.flush()#self._session.commit()

        return file_result

    def create_file(self, code_id, url, file_name, order=None, mimetype=None):

        file_location = None

        if self._base_path is not None:
            file_location_path = self.io.join(self._base_path, str(code_id))
            self.io.create_directory_tree(file_location_path)
            file_location = self.io.join(file_location_path, file_name)

        if file_location is not None:
            self.io.download_file(url, file_location)

        file_result = FileResult(code_id=code_id,
                                 file_name=file_name,
                                 order=order,
                                 mimetype=mimetype)

        self._session.add(file_result)

        self._session.flush()#self._session.commit()

        return file_result

    def get_files(self, code_id):
        code_obj = self._session.query(CodeBlock).filter_by(id=code_id).first()
        if code_obj is None:
            return []

        return code_obj.file_results

    def create_error(self, code_id, ename, evalue, traceback, order=None):
        error_result = ErrorResult(code_id=code_id,
                                   ename=ename,
                                   evalue=evalue,
                                   traceback=traceback,
                                   order=order)
        self._session.add(error_result)
        self._session.flush()#self._session.commit()
