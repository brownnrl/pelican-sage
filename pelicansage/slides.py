from pelican.generators import CachingGenerator
from pelican.contents import Content

from hovercraft.generate import generate
from argparse import Namespace
import os

import logging

logger = logging.getLogger(__name__)


class Slides(Content):
    mandatory_properties = ('title',)
    allowed_statuses = ('published', 'hidden')
    default_status = 'published'
    default_template = 'slide'

    def is_valid(self):
        return True


class SlidesGenerator(CachingGenerator):
    """Generates Hovercraft Slides"""

    def __init__(self, *args, **kwargs):
        self.slides = []
        self.hidden_slides = []
        super(SlidesGenerator, self).__init__(*args, **kwargs)

    def generate_context(self):
        pass

    def generate_output(self, writer):

        template_path = os.path.abspath(self.settings['SLIDES_THEME'])
        output_path = os.path.abspath(os.path.join(self.output_path, 'slides'))
        mathjax_cdn = 'https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.1/MathJax.js?config=TeX-MML-AM_CHTML'

        for f in self.get_files(
                self.settings['SLIDE_PATHS'],
                exclude=self.settings['SLIDE_EXCLUDES']):

            presentation = os.path.abspath(os.path.join('content', f))
            args = Namespace(presentation=presentation,
                             template=template_path,
                             targetdir=output_path,
                             css=None,
                             js=None,
                             auto_console=False,
                             slide_numbers=False,
                             skip_help=False,
                             skip_notes=False,
                             mathjax=os.environ.get('HOVERCRAFT_MATHJAX', mathjax_cdn))


            try:
                generate(args)
            except Exception as e:
                logger.error(
                    'Could not process %s\n%s', f, e)
                self._add_failed_source_path(f)
