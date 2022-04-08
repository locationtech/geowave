###############################################################################
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
#   
#  See the NOTICE file distributed with this work for additional
#  information regarding copyright ownership.
#  All rights reserved. This program and the accompanying materials
#  are made available under the terms of the Apache License,
#  Version 2.0 which accompanies this distribution and is available at
#  http://www.apache.org/licenses/LICENSE-2.0.txt
 ##############################################################################
from setuptools import setup, find_packages

setup(
        name='geowave_pyspark',
        version='${project.version}',
        url='https://locationtech.github.io/geowave/',
        packages=find_packages(),
        install_requires=['pytz', 'shapely', 'pyspark>=2.1.1,<2.3.1']
)