from __future__ import unicode_literals, print_function

import errno
import json
import logging
import os
import re
import timeit
from collections import defaultdict
from queue import Queue, Empty
from threading import Thread
from binascii import b2a_base64

import nbformat as notebookformat
from docutils import nodes
from docutils.parsers.rst import directives, Directive
from docutils.parsers.rst.directives.body import CodeBlock
from docutils.parsers.rst.directives.images import Image
from nbconvert import HTMLExporter
from nbconvert.preprocessors import ExtractOutputPreprocessor
from pelican import signals
from pelican.readers import RstReader
from traitlets import Set
from traitlets.config import Config

from .managefiles import FileManager, LanguagesStrEnum
from .pelicansageio import create_directory_tree
from .sagecell import SageCell, IPythonNotebookClient, ResultTypes
from .util import CellResult as CR, combine_results

logger = logging.getLogger(__name__)
from traceback import format_exc

try:
    from ansi2html import Ansi2HTMLConverter

    ansi_converter = lambda x: Ansi2HTMLConverter().convert(x, full=False)
except ImportError:
    ansi_converter = lambda x: str(x)

_SAGE_SETTINGS = {}

_FILE_MANAGER = None

_last_dole = 0

BASE_USER_ID_COMMENT = r'\s*id\s*:\s*(.*)$'
SCALA_USER_ID_COMMENT = re.compile(r'\s*//' + BASE_USER_ID_COMMENT)
HASKELL_USER_ID_COMMENT = re.compile(r'\s*--' + BASE_USER_ID_COMMENT)
PYTHON_USER_ID_COMMENT = re.compile(r'\s*#' + BASE_USER_ID_COMMENT)

re_comment_id_map = {'haskell': HASKELL_USER_ID_COMMENT,
                     'python': PYTHON_USER_ID_COMMENT,
                     'scala': SCALA_USER_ID_COMMENT}

c = Config()
c.HTMLExporter.preprocessors = ['pelicansage.pelicansage.ExtractAllOutputPreprocessor']


class ExtractAllOutputPreprocessor(ExtractOutputPreprocessor):
    extract_output_types = Set(
        {'image/png',
         'image/jpeg',
         'image/svg+xml',
         'application/pdf',
         'text/html',
         'text/plain'}
    ).tag(config=True)


# create the new exporter using the custom config
html_ipynb_output_exporter = HTMLExporter(config=c)


def dole_out():
    global _last_dole
    indx = _last_dole % len(_SAGE_SETTINGS['CELL_URL'])
    next_cell = _SAGE_SETTINGS['CELL_URL'][indx]
    _last_dole += 1
    return next_cell


def create_cells():
    cells = {'sage': SageCell(dole_out())}

    if _SAGE_SETTINGS['IHASKELL_URL']:
        cells['ihaskell'] = IPythonNotebookClient(_SAGE_SETTINGS['IHASKELL_URL'], timeout=60)

    if _SAGE_SETTINGS['IPYTHON_URL']:
        cells['ipython'] = IPythonNotebookClient(_SAGE_SETTINGS['IPYTHON_URL'])

    return cells


# One sage cell instance per source file.
_SAGE_CELL_INSTANCES = defaultdict(create_cells)

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
        logger.info("Evaluating %d blocks in %s", len(self.__blocks), self.__blocks[0].src.src)
        try:
            results = self.execute_blocks()
        except:
            logger.exception("Error in executing code blocks, retrying once.")
            for cell in self.__cell.values():
                cell.reset()

            results = self.execute_blocks()

        logger.info("Evaluation complete on %s.", self.__blocks[0].src.src)

        self.__queue.put((self.__blocks[0].src.src, results))

    def execute_blocks(self):
        results = []

        for block in self.__blocks:
            if block.language not in LanguagesStrEnum:
                logger.error("%s is not a supported language.", block.language.upper())
                continue

            if block.platform not in self.__cell:
                logger.error("%s not an available platform (try configuring url parameters in config file).",
                             block.platform.upper())
                continue

            cell = self.__cell[block.platform]

            response = cell.execute_request(block.content)  # TODO: pass language as parameter
            resp_results = cell.get_results_from_response(response)
            results.append((block.id, resp_results))

        return results


def process_ipynb_user_id(language, code_block_lines):
    if len(code_block_lines) > 0 and language.lower() in ('haskell', 'scala', 'python'):
        # scan to the first non-empty line
        match = re_comment_id_map[language].match(code_block_lines[0])

        if match:
            user_id = match.groups(1)[0]
            logger.error("USER_ID : %s", user_id)
            return user_id
    return None


def split_output_name(output_name):
    split = output_name.split('_')
    split = split[1:-1] + split[-1].split('.')
    return int(split[0]), int(split[1]), split[2]


def process_ipynb_output_results(cell_order, outputs):
    keys = list(outputs.keys())
    results = []
    for k in keys[:]:
        # cell order, result order, type
        c, r, t = split_output_name(k)
        if c == cell_order:
            results.append((r, t, outputs[k]))

    dict_results = defaultdict(dict)
    for r, t, o in results:
        dict_results[r][t] = o

    results = []
    for r, ts in dict_results.items():
        if 'htm' in ts:
            results.append(CR(ResultTypes.Stream,
                              r,
                              ts['htm'].decode('UTF-8'),
                              'text/html'))
        elif 'png' in ts:
            results.append(CR(ResultTypes.Stream,
                              r,
                              b2a_base64(ts['png']),
                              'image/png'))
        elif 'jpg' in ts:
            results.append(CR(ResultTypes.Stream,
                              r,
                              b2a_base64(ts['jpg']),
                              'image/jpg'))
        elif 'ksh' in ts:
            results.append(CR(ResultTypes.Stream,
                              r,
                              ts['ksh'].decode('UTF-8'),
                              'text/plain'))
        elif 'txt' in ts:
            results.append(CR(ResultTypes.Stream,
                              r,
                              ts['txt'].decode('UTF-8'),
                              'text/plain'))
        elif 'c' in ts:
            results.append(CR(ResultTypes.Stream,
                              r,
                              ts['c'].decode('UTF-8'),
                              'text/plain'))

    return results


def process_ipynb_code_results(manager, src, language, platform, cell, cell_order, nbformat, outputs):
    if 'cell_type' not in cell or cell['cell_type'] != 'code':
        return False

    code_block_lines = [l for l in cell['input' if nbformat == 3 else 'source'] if l.strip() != '']
    user_id = process_ipynb_user_id(language, code_block_lines)
    if user_id:
        code_block = ''.join(code_block_lines[1:])
    else:
        code_block = ''.join(code_block_lines)
        user_id = cell_order

    code_obj = manager.create_code(code=code_block,
                                   src=src,
                                   order=cell_order,
                                   language=language,
                                   platform=platform,
                                   user_id=user_id)

    if code_obj.last_evaluated is None:
        manager.timestamp_code(code_obj.id)
    else:
        return True

    # note the result outputs (param outputs) is of the format
    # output_{cell_index}_{result index}.type => c / htm / png / jpg
    # Let's just take htm over all other results, then png/jpg, then c
    # Where we look for a result index in that order for each result
    results = process_ipynb_output_results(cell_order, outputs)
    combined_results = combine_results(results)

    for result in combined_results:
        manager.create_result(code_obj.id,
                              result.data,
                              result.order,
                              result.mimetype)

    return True


def process_ipynb(manager, path, content_path, output_path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
        json_content = json.loads(content)
        nbformat = 3
        platform = 'ipynb'

        if json_content['nbformat'] == 4:
            nbformat = 4
        elif json_content['nbformat'] != 3:
            raise Exception("Unsupported nbformat " + str(json_content['nbformat']))

        language = 'python'
        if nbformat == 3:
            language = json_content.get('metadata', {}).get('language', None) or 'python'
        elif nbformat == 4:
            language = json_content.get('metadata', {}) \
                           .get('language_info', {}) \
                           .get('name', '').lower() or 'python'

        src = path.replace(content_path, '')

        logger.debug("Source Path: %s %s", src, path)
        # Copy the file
        src_output = os.path.join(output_path, src[1:] if src.startswith('/') else src)
        parent_dir_output = os.path.dirname(src_output)
        try:
            os.makedirs(parent_dir_output)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        manager.io.copy_file(path, src_output)

        notebook = notebookformat.reads(content, as_version=4)
        _, outputs = html_ipynb_output_exporter.from_notebook_node(notebook)
        outputs = outputs['outputs']

        cell_order = 0
        any_processed = False
        if nbformat == 3:
            for worksheet in json_content['worksheets']:
                for cell in worksheet['cells']:
                    cell_processed = process_ipynb_code_results(manager,
                                                                src,
                                                                language,
                                                                platform,
                                                                cell,
                                                                cell_order,
                                                                nbformat,
                                                                outputs)
                    if cell_processed:
                        any_processed = True

                    cell_order += 1
        else:
            for cell in json_content['cells']:
                cell_processed = process_ipynb_code_results(manager,
                                                            src,
                                                            language,
                                                            platform,
                                                            cell,
                                                            cell_order,
                                                            nbformat,
                                                            outputs)
                if cell_processed:
                    any_processed = True
                cell_order += 1

        if any_processed:
            manager.commit()

    except:
        logger.exception('Could not process {} ipython notebook.'.format(path))


def pre_read(generator):
    global _PREPROCESSING_DONE
    if _PREPROCESSING_DONE:
        SageDirective.reset_src_order()
        return

    rst_reader = RstReader(generator.settings)

    logger.info("Sage pre-processing files from the content directory")
    files = []
    for paths, excludes in ((t + '_PATHS', t + '_EXCLUDES') for t in ('ARTICLE', 'PAGE')):
        files.extend([process_file for process_file in
                      generator.get_files(
                          generator.settings[paths],
                          exclude=generator.settings[excludes],
                          extensions=False)])

    logger.debug("Files to process: %s", files)
    for f in files:
        path = os.path.abspath(os.path.join(generator.path, f))
        article = generator.readers.get_cached_data(path, None)
        if article is None:
            _, ext = os.path.splitext(os.path.basename(path))
            fmt = ext[1:]
            try:
                if fmt.lower() == 'rst':
                    rst_reader.read(path)
                elif fmt.lower() == 'ipynb':
                    process_ipynb(_FILE_MANAGER, path, _CONTENT_PATH, _SAGE_SETTINGS['OUTPUT_PATH'])
            except:  # Exception as e:
                logger.exception('Could not process {}\n{}'.format(f, format_exc()))
                continue

    # Reset the src order lookup table
    logger.info("Sage pre-processing completed.")
    _PREPROCESSING_DONE = True
    SageDirective.reset_src_order()

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

    for cells in _SAGE_CELL_INSTANCES.values():
        for cell in cells.values():
            cell.cleanup()

    # write out raw text snippets
    blks = _FILE_MANAGER.get_all_codeblocks()
    raw_base_path = os.path.join(generator.settings['OUTPUT_PATH'], 'raw/')
    create_directory_tree(raw_base_path)

    for blk in blks:
        raw_path = os.path.join(raw_base_path, '%s.txt' % (blk.id,))
        with open(raw_path, 'w') as f:
            f.write(blk.content)

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


def post_context(*args, **kwargs):
    logger.info("<<<<<<POST CONTEXT>>>>>>: %s , %s", args, kwargs)
    SageDirective.reset_src_order()


class CodeBlockEvaluator(object):
    def __init__(self, manager):
        self._manager = manager

    def process_results(self, code_id, results):
        num_names = [(x, y) for x, y in zip(ResultTypes.ALL_NUM, ResultTypes.ALL_STR)]

        pr_table = dict([(x, getattr(self, '_process_%s' % y)) for x, y in num_names])

        self._manager.timestamp_code(code_id)

        for order, result in enumerate(results):
            pr_table[result.result_type](code_id, result, order)

    def _process_image(self, code_id, image, order):
        _file_id = self._manager.create_file(code_id, image.data, os.path.split(image.data)[1], order, image.mimetype)

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
        self._manager.create_result(code_id, stream.data, order, stream.mimetype)


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
    _SAGE_SETTINGS['OUTPUT_PATH'] = pelicanobj.settings['OUTPUT_PATH']
    _SAGE_SETTINGS['DB_PATH'] = ':memory:'
    _SAGE_SETTINGS['IPYTHON_URL'] = ''
    _SAGE_SETTINGS['IHASKELL_URL'] = ''
    _CONTENT_PATH = pelicanobj.settings['PATH']

    # Alias for merge_dict
    md = lambda k, t=None: merge_dict(k, settings, _SAGE_SETTINGS, t)

    def transform_content_db(x):
        if x.startswith('{PATH}'):
            x = x.replace('{PATH}', pelicanobj.settings['PATH'])
        return x

    if settings is not None:
        md('CELL_URL')
        md('PUBLIC_CELL')
        md('FILE_BASE_PATH')
        md('DB_PATH', transform_content_db)
        md('IPYTHON_URL')
        md('IHASKELL_URL')


def _define_choice(choice1, choice2):
    return lambda arg: directives.choice(arg, (choice1, choice2))


def _get_source(directive):
    doc = directive.state_machine.document
    src = doc.source or doc.current_source
    return src


_ASSIGNED_UUIDS = {}


def _image_location(code_id, file_name):
    return '/images/sage/%s/%s' % (code_id, file_name)


def _transform_pre(content, id=None, class_=None, style=None):
    id_attribute = ' id="%(id)s"' if id else ''
    class_attribute = ' class="%(class_)s"' if class_ else ''
    template = '<code><div%s%s>%%(content)s</pre></code>' % (id_attribute,
                                                             class_attribute)
    return nodes.raw('',
                     template % {'id': id,
                                 'class_': class_,
                                 'content': content.replace('\n', '<br/>\n')},
                     format='html')


def _mod_format_permalinks(code_obj):
    if code_obj is None:
        return ''
    link_template = "<div class='permalinks'>%(link_content)s</div>"
    link_content = ''

    if code_obj.platform == 'ipynb':
        details = {'language': code_obj.language,
                   'location': code_obj.src.src}
        link_content += "i%(language)s: <a href='%(location)s'>notebook</a>" % details
    elif code_obj.platform == 'sage':
        sage_template = '<a href="%s/?z=%%s">%%s</a>' % (
            _SAGE_SETTINGS['PUBLIC_CELL'],)
        link_content += "sage: %s %s" % (
            '' if not code_obj.permalink else sage_template % (code_obj.permalink, 'block'),
            '' if not code_obj.src.permalink else sage_template % (code_obj.src.permalink, 'all'))

    link_content += " <br/>raw: <a href='/raw/%s.txt'>block</a>" % (code_obj.id,)

    return link_template % {'link_content': link_content}


def _mod_transform_result(code_id, result, order, latex=False):
    code_obj = _FILE_MANAGER.get_code(code_id)

    if result.mimetype == 'image/png':
        return _mod_transform_image(code_id, result, order)

    if result.type == ResultTypes.Error:
        result_data = ansi_converter(result.data.traceback)
    else:
        result_data = result.data

    if result.mimetype != 'text/html' and not latex:
        result_data = '<pre>%s</pre>' % (result_data,)

    if latex and result.mimetype == 'text/plain':
        result_data = '<p>$$ %s $$</p>' % (result_data,)

    return nodes.raw('',
                     """
                     <div class="code_block out_block">
                     <div class='watermark'>[out %(order)s] %(links)s</div>
                     %(result)s
                     </div>
                     """ % {'result': result_data,
                            'order': code_obj.order + 1,
                            'links': _mod_format_permalinks(code_obj)}, format='html')


def _mod_transform_image(code_id, image, order):
    code_obj = _FILE_MANAGER.get_code(code_id)
    image_node = lambda x: nodes.raw('', "<img src='%s'/>" % (x,), format='html')
    if not isinstance(image, nodes.image):
        if image.mimetype == 'image/png' and image.type != ResultTypes.Image:
            image = image_node("data:image/png;base64," + image.data)
        else:
            image = image_node(_image_location(code_id, image.data.file_name))
    else:
        image['style'] = 'margin-left: auto; margin-right: auto; display: block;'
    outer = nodes.container('',
                            nodes.raw('',
                                      "<div class='watermark'>[out %s] %s</div>" %
                                      (code_obj.order + 1, _mod_format_permalinks(code_obj)),
                                      format='html'),
                            classes=['code_block'])
    image_container = nodes.container('',
                                      image,
                                      classes=['image_container'])
    outer += image_container

    return outer


class SageDirective(CodeBlock):
    " Embed a sage cell server evaluation into posts."

    _src_order = defaultdict(lambda: 0)

    @staticmethod
    def reset_src_order():
        SageDirective._src_order.clear()

    final_argument_whitespace = False
    has_content = True

    _language = 'python'
    _platform = 'sage'

    option_spec = {'id': str,
                   'method': _define_choice('static', 'dynamic'),
                   'suppress-code': directives.flag,
                   'suppress-results': directives.flag,
                   'suppress-images': directives.flag,
                   'suppress-streams': directives.flag,
                   'suppress-errors': directives.flag,
                   'result-order': int,
                   'latex': directives.flag
                   }

    option_spec.update(CodeBlock.option_spec)

    def _check_suppress(self, name):
        for key in ('suppress-results', 'suppress-' + name):
            if key in self.options:
                return True

        return False

    def _transform_error(self, code_id, error, order):
        return _mod_transform_result(code_id, error, order)

    def _transform_stream(self, code_id, stream, order):
        if stream.mimetype == 'image/png':
            return _mod_transform_image(code_id, stream, order)
        if stream.mimetype == 'text/plain' and 'latex' in self.options:
            return _mod_transform_result(code_id, stream, order, latex=True)
        return _mod_transform_result(code_id, stream, order)

    def _transform_image(self, code_id, image, order):
        return _mod_transform_image(code_id, image, order)

    def _transform_results(self, code_id, results):

        num_names = [(x, y) for x, y in zip(ResultTypes.ALL_NUM, ResultTypes.ALL_STR)]

        def suppress(code_id, x, order):
            return None

        tr_table = dict([(x, getattr(self, '_transform_%s' % y)
        if not self._check_suppress('%ss' % y)
        else suppress) for x, y in num_names])

        transformed_nodes = []
        for order, result in enumerate(results):
            transformed_node = tr_table[result.type](code_id, result, order)
            if transformed_node is not None:
                transformed_nodes.append(transformed_node)

        if 'result-order' in self.options:
            result_order = self.options['result-order']

            try:
                result = transformed_nodes[result_order]
                transformed_nodes = [result]
            except IndexError:
                logger.error("No result of order %s for code block in src %s", result_order, self._get_source())
                return []

        return transformed_nodes

    def _get_source(self):
        return _get_source(self)

    def _get_results(self, code_obj):
        results = code_obj.results

        return results

    def _get_language(self):
        return 'python'

    def _create_codeblock(self):

        # grab the order and bump it up
        src = self._get_source()
        src = src.replace(_CONTENT_PATH, '')
        order = SageDirective._src_order[src]
        SageDirective._src_order[src] = order + 1

        user_id = None
        if 'id' in self.options:
            user_id = self.options['id'].strip().lower()

        logger.debug("USER_ID DEBUG: %s - %s - %s - %s", self.content[:1], src, order, user_id)

        code_block = '\n'.join(self.content)

        code_obj = _FILE_MANAGER.create_code(code=code_block,
                                             src=src,
                                             order=order,
                                             language=self.arguments[0],
                                             platform=self._platform,
                                             user_id=user_id)

        return code_obj

    def run(self):

        # The first pass collects up code blocks.

        # The second pass spits out results to output.

        global _PREPROCESSING_DONE

        if not self.arguments:
            self.arguments = [self._language]

        logger.debug("Creating codeblock: %s ", self._get_source())
        code_obj = self._create_codeblock()

        # First pass, reading only
        if not _PREPROCESSING_DONE:
            return []

        code_id = code_obj.id

        results = code_obj.results

        if 'suppress-code' not in self.options:
            return_nodes = super(SageDirective, self).run()
            outer = nodes.container('',
                                    nodes.raw('',
                                              "<div class='watermark'>[in %s] %s</div>" %
                                              (code_obj.order + 1, _mod_format_permalinks(code_obj)),
                                              format='html'),
                                    classes=['code_block', 'in_block'])
            outer += return_nodes[0]

            return_nodes = [outer]
        else:
            return_nodes = []

        return_nodes.extend(self._transform_results(code_id, results))

        return return_nodes


class IHaskellDirective(SageDirective):
    _language = 'haskell'
    _platform = 'ihaskell'


class IPythonDirective(SageDirective):
    _language = 'python'
    _platform = 'ipython'


class SageResultMixin(object):
    option_spec = {'file': str,
                   'order': int}

    def _get_source(self):
        return _get_source(self)

    def _get_file_reference(self, src=None, make_abs=False):

        if 'file' in self.options:
            src = self.options['file']
            make_abs = True
        elif not src:
            src = self._get_source()

        if make_abs:
            if src.startswith('/'):
                src = os.path.join(_CONTENT_PATH, src[1:])
            else:
                # grab the current directory
                src_file = self._get_source()
                # split it out
                src = os.path.join(os.path.split(src_file)[0], src)

        src = os.path.join(_CONTENT_PATH, src)
        src = os.path.abspath(src)

        if _CONTENT_PATH not in src:
            raise Exception("Source for %s is not relative to"
                            " the content directory.\n"
                            "Original Source: %s\n"
                            "File path after substitution: %s" %
                            (self.__class__.__name__, self._get_source(), src))

        this_src = self._get_source().replace(_CONTENT_PATH, '')
        src = src.replace(_CONTENT_PATH, '')

        logger.debug("Sources: %s, %s", src, this_src)
        if this_src != src:
            _FILE_MANAGER.create_reference(this_src, src)

        return src

    def _get_result_from_type(self, code_obj):
        raise NotImplementedError()

    def _get_code_result(self, src):

        code_obj = _FILE_MANAGER.get_code(src=src, user_id=self.arguments[0].strip().lower())

        if code_obj is None:
            logger.warning("Uknown code identifier <%s> in src file %s",
                           self.arguments[0].strip(), src)
            return None

        results = self._get_results_from_type(code_obj)

        order = self.options.get('order', None)

        if order is None:
            result = next(iter(results), None)  # return the first result if it exists
        else:
            result = next(filter(lambda x: x.order == order, results), None)

        if result is None:
            # TODO: Better message
            logger.warning("Tried to retrieve result but failed: <src> %s\n<code_obj> %s\n<results> %s",
                           src, code_obj, results)

        return code_obj, result

    def _go(self):
        global _PREPROCESSING_DONE

        src = self._get_file_reference()

        # First pass, reading only and creating connections
        # between referenced files
        if not _PREPROCESSING_DONE:
            return None

        return self._get_code_result(src)


class IPythonNotebook(SageDirective, SageResultMixin):
    option_spec = dict(list(SageDirective.option_spec.items()) + [('cell-order', str)])

    def _create_codeblock(self):

        global _PREPROCESSING_DONE

        if not _PREPROCESSING_DONE:
            return

        src = self._get_file_reference(self.arguments[0], make_abs=True)

        if 'id' not in self.options and 'cell-order' not in self.options:
            raise Exception("You must provide an id or order to select the correct cell in ", self.arguments[0])

        user_id = self.options['id'] if 'id' in self.options else self.options['cell-order']

        code_obj = _FILE_MANAGER.get_code(user_id=user_id, src=src)

        if code_obj is None:
            logger.error("Can not find code block with data\n%s\n%s", user_id, src)
            raise Exception("Can not find associated code block.")

        self.content = code_obj.content.split('\n')
        self.arguments[0] = code_obj.language

        return code_obj


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

        if result is None:
            logger.warning("%s has no result with the provided parameters:\n%s", self._get_file_reference(),
                           self.options)
            return []

        return [_mod_transform_result(code_obj.id, result, result.order)]


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

        if result is None:
            return []

        self.arguments[0] = _image_location(code_obj.id, result.file_name)

        return [_mod_transform_image(code_obj.id, super(SageImage, self).run()[0], result.order)]


def register():
    directives.register_directive('sage', SageDirective)
    directives.register_directive('sage-image', SageImage)
    directives.register_directive('sage-result', SageResult)
    directives.register_directive('ipynb', IPythonNotebook)
    directives.register_directive('ipython', IPythonDirective)
    directives.register_directive('ihaskell', IHaskellDirective)
    signals.article_generator_preread.connect(pre_read)
    signals.article_generator_context.connect(post_context)
    signals.initialized.connect(sage_init)
