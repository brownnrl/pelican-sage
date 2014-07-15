from setuptools import setup
from setuptools import find_packages

version = __import__('pelicansage').__version__

setup(name='pelicansage',
      author='Nelson Brown',
      author_email='brownnrl@gmail.com',
      maintainer='Nelson Brown',
      maintainer_email='brownnrl@gmail.com',
      description='Embed sage evaluation cells and results in posts.',
      license='MIT',
      version=version,
      platforms=['linux'],
      packages=find_packages(exclude=["*.tests"]),
      package_data={'': ['LICENSE', ]},
      classifiers=[
          'Development Status :: 1',
          'Operating System :: OS Independent',
          'Programming Languages :: Python :: 2.7',
          'Programming Language :: Python :: Implementation :: CPython',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Topic :: Internet :: WWW/HTTP',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Topic :: Text Processing',
      ],
      zip_safe=True,
      install_requires=['pelican>=3.3.4','ansi2html>=1.0.6']
      )
