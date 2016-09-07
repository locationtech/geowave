#!/bin/bash
source geowave-env.sh

# Configue the local host
geowave config geoserver --url "$HOSTNAME:8000"

# Add the gdelt datastore 
geowave gs addds gdelt-accumulo --datastore "gdelt" # Do we need this??

# Add layers for the point and kde representations of the data
geowave gs addlayer gdelt-accumulo
geowave gs addlayer gdelt-accumulo-out

# Add the colormap and DecimatePoints style
geowave gs addstyle colormap -sld /mnt/colormap.sld
geowave gs addstyle DecimatePoints -sld /mnt/DecimatePoints.sld

# Set the kde layer default style to colormap
geowave gs setls gdeltevent_kde --styleName colormap

