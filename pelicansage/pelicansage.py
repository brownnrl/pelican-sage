from __future__ import unicode_literals, print_function

from docutils import nodes
from docutils.parsers.rst import directives, Directive
from docutils.parsers.rst.directives.images import Image
from .sagecell import SageCell
from .managefiles import FileManager

from pelican import signals

import pprint


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

    _SAGE_CELL_INSTANCE = SageCell(_SAGE_SETTINGS['cell_url'])
    _FILE_MANAGER = FileManager()

def merge_dict(k, d1, d2, transform=None):
    if k in d1:
        d2[k] = d1[k] if transform is None else transform(d1[k])

def process_settings(pelicanobj, settings):

    global _SAGE_SETTINGS

    # Default settings
    _SAGE_SETTINGS['cell_url'] = 'http://sagecell.sagemath.org'


    # Alias for merge_dict
    md = lambda k , t=None : merge_dict(k, settings, _SAGE_SETTINGS, t)

    if settings is not None:
        md('cell_url')

def _define_choice(choice1, choice2):
    return lambda arg : directives.choice(arg, (choice1, choice2))

class SageDirective(Directive):
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

    required_arguments = 0
    optional_arguments = 2
    final_argument_whitespace = False
    has_content = True

    def __init__(self, *args, **kwargs):
        global _SAGE_CELL_INSTANCE
        super(SageDirective, self).__init__(*args, **kwargs)
        self._cell = _SAGE_CELL_INSTANCE    

    option_spec = { 'method' : _define_choice('static', 'dynamic'),
                    'show_code' : directives.flag
                    }

    def _create_pre(self, content, code_id=None):
        preamble = 'CODE ID # %(code_id)s:<br/>' if code_id else ''
        id_attribute = ' id="code_block_%(code_id)s"' if code_id else ''
        template = '%s<pre%s>%%(content)s</pre>' % (preamble, id_attribute)
        return nodes.raw('',
                         template % {'code_id' : code_id, 
                                     'content' : content}, 
                         format='html')
    def run(self):
        method_argument = 'static'
        if 'method' in self.options:
            method_argument = self.options['method']

        code_block = '\n'.join(self.content)

        resp = self._cell.execute_request(code_block)

        code_id = _FILE_MANAGER.create_code(code=code_block)

        return [self._create_pre(code_block, code_id),
                self._create_pre(pprint.pformat(resp))]

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
