# Packages up pygw so it's pip-installable
from setuptools import setup, find_packages
from maven_version import get_maven_version

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='pygw',
    author="GeoWave Contributors",
    author_email="geowave.python@gmail.com",
    description='GeoWave bindings for Python3',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://locationtech.github.io/geowave/",
    version=get_maven_version(),
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=['py4j==0.10.8.1','shapely==1.6'],
    python_requires='>=3,<=3.7' # py4j does not support python 3.8 yet
)
