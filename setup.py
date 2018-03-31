from setuptools import setup

import babar


setup(
  name='babar',
  version=babar.__version__,
  description='Elephants never forget.',
  author='Samuel Ainsworth',
  author_email='skainsworth@gmail.com',
  url='https://github.com/samuela/babar',
  packages=['babar'],
  install_requires=[],
  entry_points={
    'console_scripts': ['babar=babar.cli:main']
  }
)
