#!/usr/bin/env python
#
# from distutils.core import setup
#
# setup(name='Pycaishen',
#       version='1.0',
#       description='Market data fetching and storing module',
#       author='Mohamed Amine Guessous',
#       author_email='guessous.amine@gmail.com',
#       url='https://www.spyamine.com',
#       packages=['pycaishen'],
#      )

from setuptools import setup, find_packages

setup(name='Pycaishen',
      version='1.0',
      description='Market data fetching and storing module',
      author='Mohamed Amine Guessous',
      author_email='guessous.amine@gmail.com',
      url='https://www.spyamine.com',
      packages=find_packages(exclude=['tests','tests.*']),
      # test_suite = 'tests',
      # entry_points = {'console_scripts':['run = mypackage.module1:run']}
     )

# entry points: function made available as command-line tools
# test_suite: tests to be run by setup.py test
# install_requires : packages dependencies
# packages : to be included in distribution