#-------------------------------------------------------------------------------
# Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
# 
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License,
# Version 2.0 which accompanies this distribution and is available at
# http://www.apache.org/licenses/LICENSE-2.0.txt
#-------------------------------------------------------------------------------
#!/bin/bash
source geowave-env.sh

# Configue the local host
geowave config geoserver "$HOSTNAME:8000"

# Add the gdelt datastore 
geowave gs addds gdelt --datastore "gdelt" # Do we need this??

# Add layers for the point and kde representations of the data
geowave gs addlayer gdelt
geowave gs addlayer gdelt-kde

# Add the colormap and DecimatePoints style
geowave gs addstyle kdecolormap -sld /mnt/KDEColorMap.sld
geowave gs addstyle SubsamplePoints -sld /mnt/SubsamplePoints.sld

# Set the kde layer default style to colormap
geowave gs setls gdeltevent_kde --styleName kdecolormap

