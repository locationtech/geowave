from setuptools import setup, find_packages

setup(
        name='geowave_pyspark',
        version='${project.version}',
        url='https://locationtech.github.io/geowave/',
        packages=find_packages(),
        install_requires=['pytz', 'shapely', 'pyspark>=2.1.1,<2.3.1']
)