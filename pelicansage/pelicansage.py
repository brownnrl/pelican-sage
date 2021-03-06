from __future__ import unicode_literals, print_function

import logging
import os
import timeit
from collections import defaultdict
from queue import Queue, Empty
from threading import Thread

from docutils import nodes
from docutils.parsers.rst import directives, Directive
from docutils.parsers.rst.directives.body import CodeBlock
from docutils.parsers.rst.directives.images import Image
from pelican import signals
from pelican.readers import RstReader

from pelicansage.notebook import process_ipynb
from .managefiles import FileManager, LanguagesStrEnum
from .pelicansageio import create_directory_tree
from .managefiles import ResultTypes
from pelicansage.slides import SlidesGenerator

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


# create the new exporter using the custom config


def dole_out():
    global _last_dole
    indx = _last_dole % len(_SAGE_SETTINGS['CELL_URL'])
    next_cell = _SAGE_SETTINGS['CELL_URL'][indx]
    _last_dole += 1
    return next_cell



# One sage cell instance per source file.

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

    # write out raw text snippets
    blks = _FILE_MANAGER.get_all_codeblocks()
    raw_base_path = os.path.join(generator.settings['OUTPUT_PATH'], 'raw/')
    create_directory_tree(raw_base_path)

    for blk in blks:
        raw_path = os.path.join(raw_base_path, '%s.txt' % (blk.id,))
        with open(raw_path, 'w') as f:
            f.write(blk.content)


def post_context(*args, **kwargs):
    logger.info("<<<<<<POST CONTEXT>>>>>>: %s , %s", args, kwargs)
    SageDirective.reset_src_order()


def sage_init(pelicanobj):
    global _FILE_MANAGER

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


def add_generator(pelican_object):
    logger.error("ADDING PELICAN GENERATOR!!!")
    return SlidesGenerator


def register():
    signals.get_generators.connect(add_generator)
    directives.register_directive('notebook', IPythonNotebook)
    signals.article_generator_preread.connect(pre_read)
    signals.article_generator_context.connect(post_context)
    signals.initialized.connect(sage_init)
