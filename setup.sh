#!/bin/bash

# This script contains setup reminders.
# While it favor script execution synax, script execution is not recommended.

# UBUNTU: update Ubuntu 24.04
apt-get -yqq update
apt-get full-upgrade -yqq
apt-get autoremove -yqq

# JAVA/C: OS-Level infrastructure
apt-get install build-essential -yqq
apt-get install -y openjdk-21-jdk

# CUDA:  GPU processing infrastructure
apt-get purge 'nvidia-*'
apt-get autoremove
apt-get autoclean

wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb
dpkg -i cuda-keyring_1.1-1_all.deb
apt-get update
apt-get install cuda-toolkit-13-1 -yqq
apt-get install nvidia-open -yqq # should be 590

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
    # pip freeze | xargs pip uninstall -y. # Uncomment to clear existing packages
    pip install pyspark==4.1.* plotly==6.5.* torch==2.9.* sentence-transformers==5.2.* dash[cloud]==3.3.*\
        requests scikit-learn pandas==2.3.* openpyxl 
    pip freeze > requirements.txt
fi

# Setup project (password may be needed to use ssh keys)
git clone git@github.com:sjoshuam/reg_analysis.git

mkdir a_in b_io c_out
echo "#Project: reg_analysis" > README
echo -e ".*\na_in/\n__pycache__/" > .gitignore
echo "All rights reserved.  Software is provided for viewing purposes only, without warrenty of any kind." > LICENSE
echo "This directory holds raw source data" > a_in/PURPOSE
echo "This directory holds refined data" > b_io/PURPOSE
echo "This directory holds consumption ready data products" > c_out/PURPOSE

git add -f .gitignore a_in/PURPOSE
git add -A
git commit -m "Set up project repository"
git push origin main

# Confirm installations
unset INSTALL_STATUS
INSTALL_STATUS="==== Versions ================\n"
INSTALL_STATUS+=("[UBUNTU] $(lsb_release -r)\n")
INSTALL_STATUS+=("[CUDA] $(nvidia-smi | grep "CUDA Version")\n")
INSTALL_STATUS+=("[JAVA] $(java --version | grep openjdk)\n")
INSTALL_STATUS+=("[PYTHON] $(python3 --version)\n")
INSTALL_STATUS+=("[VENV] $(basename "$VIRTUAL_ENV")\n")
INSTALL_STATUS+=("[PANDAS] $(pip show pandas | grep Version | head -n 1)\n")
INSTALL_STATUS+=("[PYSPARK] $(pip show pyspark | grep Version | head -n 1)\n")
INSTALL_STATUS+=("[PLOTLY] $(pip show plotly | grep Version | head -n 1)\n")
INSTALL_STATUS+=("[TORCH->GPU] $(python3 -c 'import torch; print(torch.cuda.is_available())')")
echo -e ${INSTALL_STATUS[@]}
unset INSTALL_STATUS
