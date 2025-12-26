#!/bin/bash

# This script contains setup reminders.
# While it favor script execution synax, script execution is not recommended.

# UBUNTU: update Ubuntu 24.04
apt-get -yqq update
apt-get full-upgrade -yqq
apt-get autoremove -yqq

# JAVA Setup Java 21
sudo apt-get install -y openjdk-21-jdk

# SSH Keys: Generate SSH keys for GitHub access
if [ ! -d ~/.ssh ]; then
    mkdir -p ~/.ssh
    chmod 700 ~/.ssh
fi
if [ ! -f ~/.ssh/id_ed25519.pub ]; then
    ssh-keygen -t ed25519 -C $(whoami)@$(hostname)
fi

# PYTHON/GIT: Setup python 3.12 and git
apt-get install git -yqq
apt-get install python3.12 -yqq
apt-get install python3.12-venv python3-pip python3-dev -yqq

# VENV/PIP: Configure virtual environment
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

if [ -f requirements.txt ]; then
    pip install -r requirements.txt
else
    pip install install pyspark==4.1.* plotly==6.5.* pandas==2.3.* requests
    pip freeze > requirements.txt
fi

# Setup project (password may be needed to use ssh keys)
git clone git@github.com:sjoshuam/reg_analysis.git

mkdir a_in a_io a_out
echo "#Project: reg_analysis" > README
echo ".*" > .gitignore
echo "All rights reserved.  Software is provided for viewing purposes only, without warrenty of any kind." > LICENSE
echo "This directory holds raw source data" > a_in/PURPOSE
echo "This directory holds refined data" > a_io/PURPOSE
echo "This directory holds consumption ready data products" > a_out/PURPOSE

git add .gitignore README LICENSE
git commit -m "Set up project repository"

# Confirm installations
unset INSTALL_STATUS
INSTALL_STATUS="==== Versions ================\n"
INSTALL_STATUS+=("[UBUNTU] $(lsb_release -r)\n")
INSTALL_STATUS+=("[JAVA] $(java --version | grep openjdk)\n")
INSTALL_STATUS+=("[PYTHON] $(python3 --version)\n")
INSTALL_STATUS+=("[VENV] $(basename "$VIRTUAL_ENV")\n")
INSTALL_STATUS+=("[PANDAS] $(pip show pandas | grep Version | head -n 1)\n")
INSTALL_STATUS+=("[PYSPARK] $(pip show pyspark | grep Version | head -n 1)\n")
INSTALL_STATUS+=("[PLOTLY] $(pip show plotly | grep Version | head -n 1)\n")
echo -e ${INSTALL_STATUS[@]}
unset INSTALL_STATUS
