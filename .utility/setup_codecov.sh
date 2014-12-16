#!/bin/bash

cd ~
curl -s https://pypi.python.org/packages/source/p/pip/pip-1.5.6.tar.gz | tar xvz
cd pip-1.5.6
python setup.py install --user
cd ~
rm -rf pip-1.5.6

$HOME/.local/bin/pip install --user --upgrade pip distribute virtualenvwrapper

export PATH=$PATH:$HOME/.local/bin
export VIRTUALENVWRAPPER_VIRTUALENV_ARGS="--distribute"
source $HOME/.local/bin/virtualenvwrapper.sh

mkvirtualenv codecov_venv
workon codecov_venv
pip install --upgrade distribute
pip install codecov
