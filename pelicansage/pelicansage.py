from __future__ import unicode_literals, print_function

from docutils import nodes
from docutils.parsers.rst import directives, Directive
from .sagecell import SageCell

import pprint

def _define_choice(choice1, choice2):
    return lambda arg : directives.choice(arg, (choice1, choice2))

class SageDirective(Directive):
    """ Embed a sage cell server into posts.

    Usage:
    .. sage::
        :method: static

        import numpy
    
    """

    required_arguments = 0
    optional_arguments = 2
    final_argument_whitespace = False
    has_content = True

    def __init__(self, *args, **kwargs):
        super(SageDirective, self).__init__(*args, **kwargs)
        self._cell = SageCell('http://sagecell.sagemath.org/')

    
    option_spec = { 'method' : _define_choice('static', 'dynamic'),
                    'show_code' : _define_choice('true','false')}

    def run(self):
        method_argument = 'static'
        if 'method' in self.options:
            method_argument = self.options['method']

        resp = self._cell.execute_request('\n'.join(self.content))
        

        return [nodes.raw('','<pre>%s</pre>' % (pprint.pformat(resp),), format='html')]

def register():
    directives.register_directive('sage', SageDirective)
