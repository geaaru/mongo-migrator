#!/usr/bin/env python

from setuptools import setup, find_packages

from mongo_migrator import __version__

setup(
        name='mongo',
        version=__version__,
        description='Mongo Migrator',
        author='Geaaru',
        author_email='geaaru@gmail.com',
        packages=find_packages(exclude=['etc']),
        install_requires=[
            'cx_Oracle',
            'pymongo',
            'pyyaml'
        ],
        entry_points = {
            'console_scripts': [
                'mongo-migrator=mongo_migrator.migrate:main',
            ]
        }

)
