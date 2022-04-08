#-------------------------------------------------------------------------------
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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

# Add layers for the point and kde representations of the data
geowave gs layer add gdelt
geowave gs layer add gdelt-kde

# Add the colormap and DecimatePoints style
geowave gs style add kdecolormap -sld /mnt/KDEColorMap.sld
geowave gs style add SubsamplePoints -sld /mnt/SubsamplePoints.sld

# Set the kde layer default style to colormap
geowave gs style set gdeltevent_kde --styleName kdecolormap
geowave gs style set gdeltevent --styleName SubsamplePoints
