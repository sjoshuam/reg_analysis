#!/bin/bash

# This script contains simplified setup reminders for a plotly cloud-read app.

python3.12 -m venv .simple_venv
source .simple_venv/bin/activate
pip install --upgrade pip
pip install plotly==6.5.* dash[cloud]==3.3.* pandas==2.3.* openpyxl
pip freeze > c_out/requirements.txt