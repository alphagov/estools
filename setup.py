import os
import sys
from setuptools import setup, find_packages

from estools import __version__

try:
    import multiprocessing
except ImportError:
    pass

requirements = [
    'argh==0.23.0',
    'pyes==0.19.1',
]

test_requirements = [
    'mock==1.0.1',
    'nose==1.3.0',
    'freezegun==0.1.7',
]

if sys.version_info[:2] < (2, 7):
    requirements.append('argparse')

HERE = os.path.dirname(__file__)
try:
    long_description = open(os.path.join(HERE, 'README.rst')).read()
except:
    long_description = None

setup(
    name='estools',
    version=__version__,
    packages=find_packages(exclude=['test*']),

    # metadata for upload to PyPI
    author='Nick Stenning',
    author_email='nick@whiteink.com',
    maintainer='Government Digital Service',
    url='https://github.com/alphagov/estools',

    description='estools: tools for interacting with elasticsearch',
    long_description=long_description,
    license='MIT',
    keywords='sysadmin search elasticsearch templates rivers',

    install_requires=requirements,
    tests_require=test_requirements,

    test_suite='nose.collector',

    entry_points={
        'console_scripts': [
            'es-rotate=estools.command.rotate:main',
            'es-template=estools.command.template:main',
            'es-river=estools.command.river:main'
        ]
    }
)
