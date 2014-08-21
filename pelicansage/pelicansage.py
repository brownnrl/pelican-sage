from __future__ import unicode_literals, print_function

from collections import defaultdict
from docutils import nodes
from docutils.parsers.rst import directives, Directive
from docutils.parsers.rst.directives.images import Image
from docutils.parsers.rst.directives.body import CodeBlock
from .sagecell import SageCell, ResultTypes
from .managefiles import FileManager, EvaluationType


from queue import Queue, Empty
from threading import Thread

from pelican import signals
from pelican.readers import RstReader
from uuid import uuid4

import pprint

import os

import timeit

import logging
logger = logging.getLogger(__name__)
from traceback import format_exc

try:
    from ansi2html import Ansi2HTMLConverter
    ansi_converter = lambda x : Ansi2HTMLConverter().convert(x, full=False)
except ImportError:
    ansi_converter = lambda x : str(x) 

_SAGE_SETTINGS = {}

_FILE_MANAGER = None

_last_dole = 0

def dole_out():
    global _last_dole
    indx = _last_dole % len(_SAGE_SETTINGS['CELL_URL'])
    next_cell = _SAGE_SETTINGS['CELL_URL'][indx]
    _last_dole += 1
    return next_cell

# One sage cell instance per source file.
_SAGE_CELL_INSTANCES = defaultdict(lambda : SageCell(dole_out()))

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

class CellWorker(Thread):

    def __init__(self, queue, blocks, cell):
        self.__queue = queue
        self.__blocks = blocks
        self.__cell = cell
        Thread.__init__(self)

    def run(self):
        logger.info("Evaluating %d blocks in %s", len(self.__blocks),self.__blocks[0].src.src)
        try:
            results = self.execute_blocks()
        except:
            logger.exception("Error in executing code blocks, retrying once.")
            self.__cell.reset()
            results = self.execute_blocks()

        logger.info("Evaluation complete on %s.", self.__blocks[0].src.src)

        self.__queue.put((self.__blocks[0].src.src, results))

    def execute_blocks(self):
        results = []

        for block in self.__blocks:
            response = self.__cell.execute_request(block.content)
            resp_results = self.__cell.get_results_from_response(response)
            results.append((block.id, resp_results))

        return results

def pre_read(generator):
    global _PREPROCESSING_DONE
    if _PREPROCESSING_DONE:
        return

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
    logger.info("Sage pre-processing completed.")
    _PREPROCESSING_DONE = True

    # We now have all code blocks in our database, and have to evaluate
    # them.
    unevaled_blocks, references = _FILE_MANAGER.get_unevaluated_codeblocks()

    threads = []
    unique_srcs = set()
    result_queue = Queue()

    for blocks in unevaled_blocks:
        if len(blocks) == 0:
            continue
        src = blocks[0].src.src
        unique_srcs.add(src)
        cell = _SAGE_CELL_INSTANCES[src]
        threads.append(CellWorker(result_queue, blocks, cell))
    
    start_time = timeit.default_timer()
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logger.info("Results fetched in %.2f seconds.", timeit.default_timer() - start_time)

    try:
        evaluator = CodeBlockEvaluator(_FILE_MANAGER)
        while True:
            # todo: add logging statements
            src, src_results = result_queue.get_nowait()
            for block_id, block_results in src_results:
                evaluator.process_results(block_id, block_results)

    except Empty:
        pass

    for src in unique_srcs:
        _FILE_MANAGER.compute_permalink(src)

    # We are done processing results, commit them to disk
    _FILE_MANAGER.commit()

    logger.info("Results downloaded and commited in %.2f seconds.", timeit.default_timer() - start_time)


    # Find all references which are
    # Preprocessing done

    def join_path(path):
        if path.startswith('/'):
            path = path[1:]
        return os.path.abspath(os.path.join(_CONTENT_PATH, path))

    for reference in references:
        path1 = join_path(reference.src1.src)
        path2 = join_path(reference.src2.src)
        _FILE_MANAGER.io.touch_file(path1)
        _FILE_MANAGER.io.touch_file(path2)

class CodeBlockEvaluator(object):

    def __init__(self, manager):
        self._manager = manager

    def process_results(self, code_id, results):

        num_names = [(x,y) for x,y in zip(ResultTypes.ALL_NUM, ResultTypes.ALL_STR)]

        pr_table = dict([(x, getattr(self,'_process_%s'%y)) for x, y in num_names])

        self._manager.timestamp_code(code_id)

        for order, result in enumerate(results):
            pr_table[result.type](code_id, result, order)

    def _process_image(self, code_id, image, order):
        _file_id = self._manager.create_file(code_id, image.data.url, image.data.file_name, order)

    def _process_error(self, code_id, error, order):
        code_obj = self._manager.get_code(code_id)
        logger.warning("Code order #%s %sin source file %s generated exception.\n%s\n[...]\n%s", code_obj.order,
                                                                                 "(user_id %s) " % (code_obj.user_id,) if code_obj.user_id else "",
                                                                                 code_obj.src.src, 
                                                                                 code_obj.content[0:80],
                                                                                 error.data.evalue)
        self._manager.create_error(code_id, 
                                   error.data.ename,
                                   error.data.evalue,
                                   error.data.traceback,
                                   order)

    def _process_stream(self, code_id, stream, order):
        self._manager.create_result(code_id, stream.data, order)

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
    _SAGE_SETTINGS['CELL_URL'] = ['http://sagecell.sagemath.org']
    _SAGE_SETTINGS['PUBLIC_CELL'] = 'http://sagecell.sagemath.org'
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
        md('PUBLIC_CELL')
        md('FILE_BASE_PATH')
        md('DB_PATH', transform_content_db)

def _define_choice(choice1, choice2):
    return lambda arg : directives.choice(arg, (choice1, choice2))

def _get_source(directive):
    doc = directive.state_machine.document
    src = doc.source or doc.current_source
    return src

_ASSIGNED_UUIDS = {}

def _image_location(code_id, file_name):
    return '/images/sage/%s/%s' % (code_id, file_name)

def _transform_pre(content, id=None, class_=None):
    id_attribute = ' id="%(id)s"' if id else ''
    class_attribute = ' class="%(class_)s"' if class_ else ''
    template = '<code><pre%s%s>%%(content)s</pre></code>' % (id_attribute,
                                                class_attribute)
    return nodes.raw('',
            template % {'id' : id, 
                        'class_' : class_,
                        'content' : content}, 
                     format='html')

def _mod_format_permalinks(code_obj):
    if code_obj is None:
        return ''

    link_template = '<a href="%s/?z=%%s">%%s</a>' % ( 
                    _SAGE_SETTINGS['PUBLIC_CELL'],)

    return """
<div class='permalinks'>
%s
%s
</div>
""" % ('' if not code_obj.permalink else link_template % (code_obj.permalink,'blk'),
       '' if not code_obj.src.permalink else link_template % (code_obj.src.permalink,'src'))

def _mod_transform_result(code_id, result, order):
    code_obj = _FILE_MANAGER.get_code(code_id)
    permalink = ''
    src_permalink=''
    if code_obj:
        permalink = code_obj.permalink
        src_permalink = code_obj.src.permalink
    return nodes.raw('',
"""
<div class="code_block out_block">
<div class='watermark'>[out %(order)s]</div>
<pre>%(result)s</pre>
%(links)s
</div>
"""%{'result' : result,
     'order' : code_obj.order + 1,
     'links' : _mod_format_permalinks(code_obj)},format='html')

def _mod_transform_image(code_id, image, order):
    code_obj = _FILE_MANAGER.get_code(code_id)
    if not isinstance(image, nodes.image):
        image = nodes.raw('',"<span><img src='%s'/></span>"%(_image_location(code_id, image.data.file_name),), format='html')

    print(image)
    print(type(image))

    outer = nodes.container('',
                        nodes.raw('',
                                      "<div class='watermark'>[out %s]</div>" % (code_obj.order+1,),
                                      format='html'),
                            classes=['code_block'])
    outer += image 
    outer += nodes.raw('',
                       _mod_format_permalinks(code_obj), format='html'),
    return outer

class SageDirective(CodeBlock):
    " Embed a sage cell server evaluation into posts."

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

    def _check_suppress(self, name):
        for key in ('suppress-results', 'suppress-' + name):
            if key in self.options:
                return True

        return False

    def _transform_error(self, code_id, error, order):
        return _mod_transform_result(code_id, ansi_converter(error.data.traceback), order)

    
    def _transform_stream(self, code_id, stream, order):
        return _mod_transform_result(code_id, stream.data, order)


    def _transform_image(self, code_id, image, order):
        return _mod_transform_image(code_id, image, order)

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


    def _get_source(self):
        return _get_source(self)

    def _get_sage_instance(self):
        global _SAGE_CELL_INSTANCES
        src = self._get_source()

        return _SAGE_CELL_INSTANCES[src]

    def _get_results(self, code_obj):
        results = code_obj.results

        return results

    def run(self):

        # The first pass collects up code blocks.

        # The second pass spits out results to output.

        global _PREPROCESSING_DONE

        if not self.arguments:
            self.arguments = ['python']
        
        if 'number-lines' not in self.options:
            self.options['number-lines'] = None

        # grab the order and bump it up
        src = self._get_source()
        src = src.replace(_CONTENT_PATH, '')
        order = self._src_order[src]
        self._src_order[src] = order + 1

        user_id = None
        if 'id' in self.options:
            user_id = self.options['id'].strip().lower()

        code_block = '\n'.join(self.content)

        code_obj = _FILE_MANAGER.create_code(code=code_block,
                                             src=src,
                                             order=order,
                                             user_id=user_id)

        # First pass, reading only
        if not _PREPROCESSING_DONE:
            return []

        code_id = code_obj.id

        results = code_obj.results

        if 'suppress_code' not in self.options:
            return_nodes = super(SageDirective, self).run()
            outer = nodes.container('',
                                    nodes.raw('',
                                              "<div class='watermark'>[in %s]</div>" % (code_obj.order+1,),
                                              format='html'),
                                    classes=['code_block', 'in_block'])
            outer += return_nodes[0]
            outer += nodes.raw('',
                               _mod_format_permalinks(code_obj), format='html'),
            return_nodes = [outer]
        else:
            return_nodes = []

        return_nodes.extend(self._transform_results(code_id, results))

        return return_nodes

class SageResultMixin(object):

    option_spec = { 'file' : str,
                    'order' : int }


    def _get_source(self):
        return _get_source(self)

    def _get_file_reference(self):

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

        this_src = self._get_source().replace(_CONTENT_PATH, '')
        src = src.replace(_CONTENT_PATH, '')

        if this_src != src:
            _FILE_MANAGER.create_reference(this_src, src)

        return src

    def _get_result_from_type(self, code_obj):
        raise NotImplementedError()


    def _get_code_result(self, src):

        code_obj = _FILE_MANAGER.get_code(src=src,user_id=self.arguments[0].strip().lower())

        if code_obj is None:
            logger.warning("Uknown code identifier <%s> in src file %s", 
                                self.arguments[0].strip(), src)
            return None 

        results = self._get_results_from_type(code_obj) 

        order = self.options.get('order', None)

        if order is None:
            result = next(iter(results),None) # return the first result if it exists
        else:
            result = next(filter(lambda x: x.order == order, results), None)

        if result is None:
            # TODO: Better message
            logger.warning("Tried to retrieve result but failed.")

        return code_obj, result

    def _go(self):
        global _PREPROCESSING_DONE

        src = self._get_file_reference()

        # First pass, reading only and creating connections
        # between referenced files
        if not _PREPROCESSING_DONE:
            return None 

        return self._get_code_result(src)

class SageResult(SageResultMixin, Directive):

    option_spec = SageResultMixin.option_spec
    required_arguments = 1
    final_argument_whitespace = True

    def _get_results_from_type(self, code_obj):
        return code_obj.stream_results

    def run(self):
        result = self._go()

        if result is None:
            return []

        code_obj, result = result

        return [_mod_transform_result(code_obj.id, result.data, result.order)]

class SageImage(SageResultMixin, Image):

    option_spec = dict(list(Image.option_spec.items()) +
                       list(SageResultMixin.option_spec.items()))
    
    def _get_results_from_type(self, code_obj):
        return code_obj.file_results

    def run(self):
        result = self._go()

        if result is None:
            return []

        code_obj, result = result

        self.arguments[0] = _image_location(code_obj.id, result.file_name)

        return [_mod_transform_image(code_obj.id, super(SageImage, self).run()[0], result.order)]


def register():
    directives.register_directive('sage', SageDirective)
    directives.register_directive('sage-image', SageImage)
    directives.register_directive('sage-result', SageResult)
    signals.article_generator_preread.connect(pre_read)
    signals.initialized.connect(sage_init)
