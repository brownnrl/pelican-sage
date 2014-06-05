from __future__ import unicode_literals

from docutils import nodes
from docutils.parsers.rst import directives, Directive

class SageDirective(Directive):
    """ Embed a sage cell server into posts.

    Usage:
    .. sage::
        :method: static

        import numpy
    
    """

    def method(argument):
        """Conversion function for the "method" option."""
        return directives.choice(argument, ('static', 'dynamic'))
    
    required_arguments = 0
    optional_arguments = 1
    option_spec = { 'method' : method }

    final_argument_whitespace = False
    has_content = True

    def run(self):
        method_argument = 'static'
        if 'method' in self.options:
            method_argument = self.options['method']

        data = "<p>TEST %s</p>" % (method_argument,)

        return [nodes.raw('', data, format='html')]

def register():
    directives.register_directive('sage', SageDirective)
