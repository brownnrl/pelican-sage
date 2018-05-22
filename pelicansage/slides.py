from pelican.generators import CachingGenerator
from pelican.contents import Content

from io import StringIO

from itertools import chain

from hovercraft.parse import HovercraftReader, rst2xml, Writer
from hovercraft.generate import rst2html
from hovercraft.template import Template
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
        all_slides = []
        hidden_slides = []

        logger.debug("CALLING SLIDES GENERATOR CONTEXT")

        for f in self.get_files(
                self.settings['SLIDE_PATHS'],
                exclude=self.settings['SLIDE_EXCLUDES']):
            slide_page = self.get_cached_data(f, None)
            if slide_page is None:
                try:
                    slide_page, _ = rst2html(os.path.join(self.path, f), Template())
                    slide_page = slide_page.decode("UTF-8")
                    """
                    slide_page = self.readers.read_file(
                        base_path=self.path,
                        path=f,
                        content_class=Slides,
                        context=self.context,
                        preread_signal=None,
                        context_signal=None,
                        context_sender=None)
                    """
                except Exception as e:
                    logger.error(
                        'Could not process %s\n%s',
                        f, e, exec_info=self.settings.get('DEBUG', False))
                    self._add_failed_source_path(f)

            all_slides.append(slide_page)
            """
            if not slide_page.is_valid():
                self._add_failed_source_path(f)
                continue

            if slide_page.status == "published":
                all_slides.append(slide_page)
            elif slide_page.status == "hidden":
                hidden_slides.append(slide_page)
            self.add_source_path(slide_page)
            self._update_context(('slides', 'hidden_slides'))
            self.save_cache()
            self.readers.save_cache()
            """

        self.slides = all_slides
        self.hidden_slides = hidden_slides

    def generate_output(self, writer):
        default_template = Template()
        logger.error("CALLING GENERATE OUTPUT")

        for idx, slides in enumerate(chain(self.slides, self.hidden_slides)):
            with open("contentslides%s.html" % (idx,), "w") as f:
                f.write(slides)

