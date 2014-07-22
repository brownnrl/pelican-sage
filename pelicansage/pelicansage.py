from __future__ import unicode_literals, print_function

from collections import defaultdict
from docutils import nodes
from docutils.parsers.rst import directives, Directive
from docutils.parsers.rst.directives.images import Image
from docutils.parsers.rst.directives.body import CodeBlock
from .sagecell import SageCell, ResultTypes
from .managefiles import FileManager, EvaluationType

from pelican import signals

import pprint

import os

try:
    from ansi2html import Ansi2HTMLConverter
    ansi_converter = Ansi2HTMLConverter().convert
except ImportError:
    ansi_converter = lambda x : '<pre>%s</pre>' % (x,)

_SAGE_SETTINGS = {}

_FILE_MANAGER = None

_SAGE_CELL_INSTANCE = None

def sage_init(pelicanobj):

    global _FILE_MANAGER
    global _SAGE_CELL_INSTANCE

    try:
        settings = pelicanobj.settings['SAGE']
    except:
        settings = None

    process_settings(pelicanobj, settings)

    _SAGE_CELL_INSTANCE = SageCell(_SAGE_SETTINGS['CELL_URL'])
    _FILE_MANAGER = FileManager(location=_SAGE_SETTINGS['DB_PATH'],
                                base_path=_SAGE_SETTINGS['FILE_BASE_PATH'])

def merge_dict(k, d1, d2, transform=None):
    if k in d1:
        d2[k] = d1[k] if transform is None else transform(d1[k])

def process_settings(pelicanobj, settings):

    global _SAGE_SETTINGS

    # Default settings
    _SAGE_SETTINGS['CELL_URL'] = 'http://sagecell.sagemath.org'
    _SAGE_SETTINGS['FILE_BASE_PATH'] = os.path.join(pelicanobj.settings['OUTPUT_PATH'], 'images/sage')
    _SAGE_SETTINGS['DB_PATH'] = ':memory:'


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

    def __init__(self, *args, **kwargs):
        global _SAGE_CELL_INSTANCE
        super(SageDirective, self).__init__(*args, **kwargs)
        self._cell = _SAGE_CELL_INSTANCE    

    option_spec = { 'method'           : _define_choice('static', 'dynamic'),
                    'suppress_code'    : directives.flag,
                    'suppress_results' : directives.flag,
                    'suppress_images'  : directives.flag,
                    'suppress_streams' : directives.flag,
                    'suppress_errors'  : directives.flag
                    }

    option_spec.update(CodeBlock.option_spec)

    def _create_pre(self, content, code_id=None):
        preamble = 'CODE ID # %(code_id)s:<br/>' if code_id else ''
        id_attribute = ' id="code_block_%(code_id)s"' if code_id else ''
        template = '%s<pre%s>%%(content)s</pre>' % (preamble, id_attribute)
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

    def run(self):

        if not self.arguments:
            self.arguments = ['python']
        
        if 'number-lines' not in self.options:
            self.options['number-lines'] = None

        method_argument = 'static'
        if 'method' in self.options:
            method_argument = self.options['method']

        # grab the order and bump it up
        
        doc = self.state_machine.document
        src = doc.source or doc.current_source
        order = self._src_order[src]
        self._src_order[src] = order + 1

        code_block = '\n'.join(self.content)

        code_obj = _FILE_MANAGER.create_code(code=code_block,
                                             src=src,
                                             order=order)
        code_id = code_obj.id

        if code_obj.last_evaluated is None:
            resp = self._cell.execute_request(code_block)
            _FILE_MANAGER.timestamp_code(code_obj.id)
            results = self._cell.get_results_from_response(resp)
            self._process_results(code_id, results)
        else:
            results = code_obj.results

        if 'suppress_code' not in self.options:
            return_nodes = super(SageDirective, self).run()
        else:
            return_nodes = []

        return_nodes.extend(self._transform_results(code_id, results))

        return return_nodes

class SageImage(Image):
    
    def run(self):
        print("RUNNING THE IMAGE DIRECTIVE:", self.arguments[0])
        self.arguments[0] = "http://static3.businessinsider.com/image/52cddfb169beddee2a6c2246/the-29-coolest-us-air-force-images-of-the-year.jpg"
        return super(SageImage, self).run()

def register():
    directives.register_directive('sage', SageDirective)
    directives.register_directive('sage-image', SageImage)
    signals.initialized.connect(sage_init)
    # I don't believe we need to connect to the content_object_init
    # as we handle this in the directive... revisit.
