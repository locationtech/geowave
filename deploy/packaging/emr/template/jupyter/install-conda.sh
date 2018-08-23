#!/usr/bin/env bash

CONDA_DL_LOC=${1-$HOME/miniconda.sh}
CONDA_INSTALL_LOC=${2-$HOME/conda/}


# Download latest conda (Python 3.6.1) to root install location
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O "$CONDA_DL_LOC"

# Modify the file permissions to allow execution within this shell
chmod +x ${CONDA_DL_LOC}

# Install miniconda and output directory to /opt/conda
${CONDA_DL_LOC} -bfp ${CONDA_INSTALL_LOC}

# Add Conda to the path so all users with shell can see conda
printf "export PATH=${CONDA_INSTALL_LOC}bin:"'$PATH' | sudo tee -a /etc/profile.d/conda.sh
# setup python 3.6 in the master and workers
printf "\nexport PYSPARK_PYTHON=${CONDA_INSTALL_LOC}bin/python3.6" | sudo tee -a /etc/profile.d/conda.sh
printf "\nexport PYSPARK_DRIVER_PYTHON=${CONDA_INSTALL_LOC}bin/python3.6" | sudo tee -a /etc/profile.d/conda.sh
# This was added because Upstart doesn't capture user environment variables before loading jupyter
printf "\nexport HOSTNAME=$HOSTNAME" | sudo tee -a /etc/profile.d/conda.sh

sudo chmod +x /etc/profile.d/conda.sh

source /etc/profile.d/conda.sh

# Set config options to install dependencies properly
${CONDA_INSTALL_LOC}/bin/conda config --system --set always_yes yes --set changeps1 no
${CONDA_INSTALL_LOC}/bin/conda config --system -f --add channels conda-forge

# Install dependencies used for geowave + pixiedust
${CONDA_INSTALL_LOC}/bin/conda install matplotlib numpy pandas pyyaml requests shapely folium owslib nbconvert

# Install pip dependencies
${CONDA_INSTALL_LOC}/bin/pip install pixiedust oauthenticator ipywidgets ipyleaflet geomet pandas shapely folium owslib

rm -f ${CONDA_DL_LOC}