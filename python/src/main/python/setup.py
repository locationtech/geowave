# Packages up pygw so it's pip-installable
from setuptools import setup, find_packages

with open('README.md', 'r') as fh:
    long_description = fh.read()

def get_version():
    try:
        from maven_version import get_maven_version
        version = get_maven_version()
    except ModuleNotFoundError:
        # If maven version isn't found, it must be from the distribution
        from pkg_resources import get_distribution
        from pkg_resources import DistributionNotFound
        version = get_distribution('pygw').version
    return version

setup(
    name='pygw',
    author='GeoWave Contributors',
    author_email='geowave.python@gmail.com',
    description='GeoWave bindings for Python3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://locationtech.github.io/geowave/',
    project_urls={
        'Documentation': 'https://locationtech.github.io/geowave/pydocs/',
        'Source': 'https://github.com/locationtech/geowave/tree/master/python/src/main/python',
    },
    version=get_version(),
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.0',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    install_requires=['py4j==0.10.8.1','shapely==1.6'],
    python_requires='>=3,<3.8' # py4j does not support python 3.8 yet
)
