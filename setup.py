# -*- coding: utf-8 -*-

import os

from setuptools import setup
from setuptools import find_packages


def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()


setup(
    name='zodb_nosql',
    version='0.1.dev0',
    description='',
    long_description=read('README.rst') +
                     read('HISTORY.rst') +
                     read('LICENSE'),
    classifiers=[
        "Programming Language :: Python",
    ],
    author='Nathan Van Gheem',
    author_email='nathan@vangheem.us',
    url='',
    license='BSD',
    packages=find_packages(),
    install_requires=[
        'setuptools',
        'requests',
        'pycouchdb'
    ],
    extras_require={
    },
    entry_points="""
    """,
    include_package_data=True,
    zip_safe=False,
)
