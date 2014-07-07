from __future__ import unicode_literals, print_function

from docutils import nodes
from docutils.parsers.rst import directives, Directive

import urllib, urllib2, cookielib

class SageDirective(Directive):
    """ Embed a sage cell server into posts.

    Usage:
    .. sage::
        :method: static

        import numpy
    
    """


    required_arguments = 0
    optional_arguments = 1
    final_argument_whitespace = False
    has_content = True

    _cookie_jar = cookielib.CookieJar()
    _opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(_cookie_jar))
    urllib2.install_opener(_opener)

    def __init__(self, *args, **kwargs):
        super(SageDirective, self).__init__(*args, **kwargs)

    def method(argument):
        """Conversion function for the "method" option."""
        return directives.choice(argument, ('static', 'dynamic'))
    
    option_spec = { 'method' : method }

    def run(self):
        method_argument = 'static'
        if 'method' in self.options:
            method_argument = self.options['method']

        code = """
print 1 + 1
import matplotlib.pyplot as plt
plt.plot([1,2,3,4])
plt.ylabel('some numbers')
plt.show()
"""
        url_1 = 'https://aleph.sagemath.org/service'
        data = urllib.urlencode({"accepted_tos":"true",
                                 "code": code})

        req = urllib2.Request(url_1, data)
        rsp = urllib2.urlopen(req)
        content = rsp.read()

        print(content)

        return [nodes.raw('', content, format='html')]

def register():
    directives.register_directive('sage', SageDirective)
