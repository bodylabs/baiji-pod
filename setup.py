# https://coderwall.com/p/qawuyq
# Thanks James.

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ''
    print 'warning: pandoc or pypandoc does not seem to be installed; using empty long_description'

import os
requirements_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'requirements.txt')
with open(requirements_file, 'r') as f:
    install_requires = [x.strip() for x in f.readlines()]

import importlib
from setuptools import setup

setup(
    name='baiji-pod',
    version=importlib.import_module('baiji.pod').__version__,
    author='Body Labs',
    author_email='alex@bodylabs.com, paul.melnikow@bodylabs.com',
    description='Static caching of files from Amazon S3, using baiji',
    long_description=long_description,
    url='https://github.com/bodylabs/baiji-pod',
    license='Apache',
    packages=[
        'baiji',
        'baiji/pod',
        'baiji/pod/runners',
        'baiji/pod/util',
        'baiji/pod/versioned',
    ],
    scripts=[
        'bin/sc',
        'bin/vc',
    ],
    install_requires=install_requires,
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ]
)
