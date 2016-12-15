#!/bin/bash
source geowave-env.sh

# Configue the local host
geowave config geoserver --url "$HOSTNAME:8000"

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

