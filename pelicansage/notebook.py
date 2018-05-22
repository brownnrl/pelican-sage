import errno
import json
import os
import re
from collections.__init__ import defaultdict
from uuid import uuid4

import nbformat as notebookformat
from nbconvert import HTMLExporter
from nbconvert.preprocessors import ExtractOutputPreprocessor
from traitlets import Set
from traitlets.config import Config

from pelicansage.managefiles import ResultTypes
from pelicansage.util import CellResult as CR, combine_results

import logging

logger = logging.getLogger(__name__)

class ExtractAllOutputPreprocessor(ExtractOutputPreprocessor):
    extract_output_types = Set(
        {'image/png',
         'image/jpeg',
         'image/svg+xml',
         'application/pdf',
         'text/html',
         'text/plain'}
    ).tag(config=True)


c = Config()
c.HTMLExporter.preprocessors = ['pelicansage.notebook.ExtractAllOutputPreprocessor']

html_ipynb_output_exporter = HTMLExporter(config=c)
BASE_USER_ID_COMMENT = r'\s*id\s*:\s*(.*)$'
HASKELL_USER_ID_COMMENT = re.compile(r'\s*--' + BASE_USER_ID_COMMENT)
PYTHON_USER_ID_COMMENT = re.compile(r'\s*#' + BASE_USER_ID_COMMENT)
SCALA_USER_ID_COMMENT = re.compile(r'\s*//' + BASE_USER_ID_COMMENT)
re_comment_id_map = {'haskell': HASKELL_USER_ID_COMMENT,
                     'python': PYTHON_USER_ID_COMMENT,
                     'scala': SCALA_USER_ID_COMMENT}


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
            results.append(CR(ResultTypes.Image,
                              r,
                              ts['png'],
                              'image/png'))
        elif 'jpg' in ts:
            results.append(CR(ResultTypes.Image,
                              r,
                              ts['jpg'],
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
        elif 'bat' in ts:
            results.append(CR(ResultTypes.Stream,
                              r,
                              ts['bat'].decode('UTF-8'),
                              'text/plain'))
        else:
            logger.error("Unknown result type %s\n%s", repr(ts)[:40], repr(r)[:40])

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
        if result.result_type == ResultTypes.Image:
            uuid = uuid4()
            ext = 'dat'
            if result.mimetype == 'image/png':
                ext = 'png'
            elif result.mimetype == 'image/jpg':
                ext = 'jpg'
            manager.save_file(code_obj.id,
                              result.data,
                              '{}_{}.{}'.format(result.order, uuid, ext),
                              result.order,
                              result.mimetype)
        else:
            logger.debug("creating result for %s <%s, %s>: %s",
                         code_obj.id,
                         result.order,
                         result.mimetype,
                         str(result.data)[0:50])
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


def process_ipynb_user_id(language, code_block_lines):
    if len(code_block_lines) > 0 and language.lower() in ('haskell', 'scala', 'python'):
        # scan to the first non-empty line
        match = re_comment_id_map[language].match(code_block_lines[0])

        if match:
            user_id = match.groups(1)[0]
            return user_id
    return None


