# Packages up pygw so it's pip-installable
from setuptools import setup

setup(
    name='pygw',
    description='GeoWave bindings for Python3',
    version='1.0.0rc2',
    packages=['pygw',],
    install_requires=['py4j==0.10.8.1','shapely==1.6'],
    python_requires='>=3,<=3.7' # py4j does not support python 3.8 yet
)
