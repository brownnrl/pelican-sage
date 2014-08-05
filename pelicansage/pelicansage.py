from __future__ import unicode_literals, print_function

from collections import defaultdict
from docutils import nodes
from docutils.parsers.rst import directives, Directive
from docutils.parsers.rst.directives.images import Image
from docutils.parsers.rst.directives.body import CodeBlock
from .sagecell import SageCell, ResultTypes
from .managefiles import FileManager, EvaluationType


from pelican import signals
from pelican.readers import RstReader
from uuid import uuid4

import pprint

import os

import logging
logger = logging.getLogger(__name__)
from traceback import format_exc

try:
    from ansi2html import Ansi2HTMLConverter
    ansi_converter = Ansi2HTMLConverter().convert
except ImportError:
    ansi_converter = lambda x : '<pre>%s</pre>' % (x,)

_SAGE_SETTINGS = {}

_FILE_MANAGER = None

# One sage cell instance per source file.
_SAGE_CELL_INSTANCES = {}

_CONTENT_PATH = None 

# We have to turn pelican into a two pass system if we want to have result
# interdependencies between files executed correctly, and also for those results
# to be generated asynchronously.

# The first pass will read each sage code block and populate the database
# determining the relationships between code blocks, execute the code blocks in
# the appropriate namespaces (asynchronously), collect up the results, and
# cache them appropriately.

# The first pass will also determine which files had references to results
# in other files, and touch them so the result references update correctly
# during the second pass.

# The second pass will actually use the results for output.
_PREPROCESSING_DONE = False

def pre_read(generator):
    global _PREPROCESSING_DONE
    if _PREPROCESSING_DONE:
        return
    else:
        _PREPROCESSING_DONE = True

    rst_reader = RstReader(generator.settings)

    logger.info("Sage pre-processing files from the content directory")
    files = [article_file for article_file in 
                generator.get_files(
                generator.settings['ARTICLE_PATHS'],
                exclude=generator.settings['ARTICLE_EXCLUDES'])]
    for f in files:
        path = os.path.abspath(os.path.join(generator.path,f))
        article = generator.readers.get_cached_data(path, None)
        if article is None:
            _, ext = os.path.splitext(os.path.basename(path))
            fmt = ext[1:]
            try:
                if fmt.lower() == 'rst':
                    rst_reader.read(path)
            except:# Exception as e:
                logger.exception('Could not process {}\n{}'.format(f, format_exc()))
                continue
    # Reset the src order lookup table
    SageDirective._src_order = defaultdict(lambda : 0) 
    logger.info("Sage processing completed.")

def sage_init(pelicanobj):

    global _FILE_MANAGER
    global _SAGE_CELL_INSTANCES

    try:
        settings = pelicanobj.settings['SAGE']
    except:
        settings = None

    process_settings(pelicanobj, settings)

    _FILE_MANAGER = FileManager(location=_SAGE_SETTINGS['DB_PATH'],
                                base_path=_SAGE_SETTINGS['FILE_BASE_PATH'])

def merge_dict(k, d1, d2, transform=None):
    if k in d1:
        d2[k] = d1[k] if transform is None else transform(d1[k])

def process_settings(pelicanobj, settings):

    global _SAGE_SETTINGS
    global _CONTENT_PATH

    # Default settings
    _SAGE_SETTINGS['CELL_URL'] = 'http://sagecell.sagemath.org'
    _SAGE_SETTINGS['FILE_BASE_PATH'] = os.path.join(pelicanobj.settings['OUTPUT_PATH'], 'images/sage')
    _SAGE_SETTINGS['DB_PATH'] = ':memory:'
    _CONTENT_PATH = pelicanobj.settings['PATH']


    # Alias for merge_dict
    md = lambda k , t=None : merge_dict(k, settings, _SAGE_SETTINGS, t)

    def transform_content_db(x):
        if x.startswith('{PATH}'):
            x = x.replace('{PATH}',pelicanobj.settings['PATH'])
        return x 

    if settings is not None:
        md('CELL_URL')
        md('FILE_BASE_PATH')
        md('DB_PATH', transform_content_db)

def _define_choice(choice1, choice2):
    return lambda arg : directives.choice(arg, (choice1, choice2))

def _get_source(directive):
    doc = directive.state_machine.document
    src = doc.source or doc.current_source
    return src

def _detect_filename(path, src):

    if len(path.split('||'))!=2:
        # The user is not passing path + identifier
        # so we have to relate it back to the current file
        path = '%s||%s' % (src.replace(_CONTENT_PATH,''),path)

        if path.startswith('/'):
            path = path[1:]

    return path

_ASSIGNED_UUIDS = {}

class SageDirective(CodeBlock):
    """ Embed a sage cell server into posts.

    Usage:
    .. sage::
        :method: static       # Executed once on page generation,
                              # dynamic to turn into a javascript call
        :edit: false          # true will make it a "standard" sage cell
        :hide-code: true      # code block will be hidden from view (open with a small button)
        :suppress-code: false # true would remove the small button
        :hide-results: false  # keep the results from showing up sequentially
        :hide-images: false   # keep the images from showing up sequentially

        import numpy
    
    """

    _src_order = defaultdict(lambda : 0) 

    final_argument_whitespace = False
    has_content = True

    option_spec = { 'id'               : str,
                    'method'           : _define_choice('static', 'dynamic'),
                    'suppress-code'    : directives.flag,
                    'suppress-results' : directives.flag,
                    'suppress-images'  : directives.flag,
                    'suppress-streams' : directives.flag,
                    'suppress-errors'  : directives.flag
                    }

    option_spec.update(CodeBlock.option_spec)

    def _create_pre(self, content, code_id=None):
        preamble = 'CODE ID # %(code_id)s:<br/>' if code_id else ''
        id_attribute = ' id="code_block_%(code_id)s"' if code_id else ''
        template = '%s<pre%s>%%(content)s</pre>' % (preamble, id_attribute)
        uuid = uuid4()
        return nodes.raw('',
                template % {'code_id' : code_id, 
                            'content' : content}, 
                         format='html')

    def _check_suppress(self, name):
        for key in ('suppress_results', 'suppress_' + name):
            if key in self.options:
                return True

        return False

    def _process_error(self, code_id, error, order):
        _FILE_MANAGER.create_error(code_id, 
                                   error.data.ename,
                                   error.data.evalue,
                                   error.data.traceback,
                                   order)

    def _transform_error(self, code_id, error, order):
        return nodes.raw('',
                         ansi_converter(error.data.traceback),
                         format='html')

    def _process_stream(self, code_id, stream, order):
        _FILE_MANAGER.create_result(code_id, stream.data, order)
    
    def _transform_stream(self, code_id, stream, order):
        return self._create_pre(stream.data)

    def _process_image(self, code_id, image, order):
        _file_id = _FILE_MANAGER.create_file(code_id, image.data.url, image.data.file_name, order)

    def _transform_image(self, code_id, image, order):
        return nodes.image(uri='/images/sage/%s/%s' % (code_id, image.data.file_name))

    def _transform_results(self, code_id, results):

        num_names = [(x,y) for x,y in zip(ResultTypes.ALL_NUM, ResultTypes.ALL_STR)]

        def suppress(code_id, x, order):
            return None

        tr_table = dict([(x,getattr(self,'_transform_%s'%y)
                            if not self._check_suppress('%ss'%y)
                            else suppress) for x,y in num_names])

        transformed_nodes = []
        for order, result in enumerate(results):
            transformed_node = tr_table[result.type](code_id, result, order)
            if transformed_node is not None:
                transformed_nodes.append(transformed_node)
        return transformed_nodes


    def _process_results(self, code_id, results):

        num_names = [(x,y) for x,y in zip(ResultTypes.ALL_NUM, ResultTypes.ALL_STR)]

        pr_table = dict([(x, getattr(self,'_process_%s'%y)) for x, y in num_names])

        for order, result in enumerate(results):
            pr_table[result.type](code_id, result, order)

    def _get_source(self):
        return _get_source(self)

    def _get_sage_instance(self):
        global _SAGE_CELL_INSTANCES
        src = self._get_source()

        if src in _SAGE_CELL_INSTANCES:
            return _SAGE_CELL_INSTANCES[src]

        _SAGE_CELL_INSTANCES[src] = SageCell(_SAGE_SETTINGS['CELL_URL'])

        return _SAGE_CELL_INSTANCES[src]

    def _get_results(self, code_obj):
        # We need to evaluate all prior code blocks which have not been
        # evaluated.

        chain = _FILE_MANAGER.get_code_block_chain(code_obj)

        if code_obj.last_evaluated is None:
            cell = self._get_sage_instance()
            resp = cell.execute_request(code_obj.content)
            _FILE_MANAGER.timestamp_code(code_obj.id)
            results = cell.get_results_from_response(resp)
            self._process_results(code_obj.id, results)

        results = code_obj.results

        return results

    def run(self):

        if not self.arguments:
            self.arguments = ['python']
        
        if 'number-lines' not in self.options:
            self.options['number-lines'] = None

        method_argument = 'static'
        if 'method' in self.options:
            method_argument = self.options['method']

        # grab the order and bump it up
        
        src = self._get_source()
        order = self._src_order[src]
        self._src_order[src] = order + 1

        user_id = None
        if 'id' in self.options:
            user_id = self.options['id']

        code_block = '\n'.join(self.content)

        code_obj = _FILE_MANAGER.create_code(code=code_block,
                                             src=src,
                                             order=order,
                                             user_id=user_id)
        code_id = code_obj.id

        print((code_id, src))

        # Here we wait
        return [nodes.raw('','\n%s\n'%(uuid4(),),format='html')]

        results = self._get_results(code_obj)

        if 'suppress_code' not in self.options:
            return_nodes = super(SageDirective, self).run()
        else:
            return_nodes = []

        return_nodes.extend(self._transform_results(code_id, results))

        return return_nodes

class SageImage(Image):

    option_spec = dict(list(Image.option_spec.items()) +
                       [('file',str), ('order', int)])

    def _get_source(self):
        return _get_source(self)
    
    def run(self):

        if 'file' in self.options:
            src = self.options['file']
            
            if src.startswith('/'):
                src = os.path.join(_CONTENT_PATH, src[1:])
            else:
                # grab the current directory
                src_file = self._get_source()
                # split it out
                src = os.path.join(os.path.split(src_file)[0], src)
        else:
            src = self._get_source()

        src = os.path.join(_CONTENT_PATH, src)
        src = os.path.abspath(src)

        if _CONTENT_PATH not in src:
            raise Exception("Source for sage image directive is not relative to"
                            " the content directory.\n"
                            "Original Source: %s\n"
                            "File path after substitution: %s" % 
                            (self._get_source(), src))

        src = src.replace(_CONTENT_PATH, '')

        user_id = ':'.join((src, self.arguments[0]))

        code_obj = _FILE_MANAGER.get_code(user_id=user_id)

        
        return []


        results = code_obj.results

        order = self.options.get('order', None)

        if order is None:
            image = next(filter(lambda x: x.type == ResultTypes.Image, results), None)

        return [_transform_image(self, code_obj.id, image, image.order)]


def register():
    directives.register_directive('sage', SageDirective)
    directives.register_directive('sage-image', SageImage)
    signals.article_generator_preread.connect(pre_read)
    signals.initialized.connect(sage_init)
