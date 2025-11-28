#!/bin/bash

sudo add-apt-repository ppa:sumo/stable
sudo apt-get update
sudo apt-get install sumo sumo-tools sumo-doc

cd ~ 
export SUMO_HOME=/usr/share/sumo
cd $SUMO_HOME

# Installing Google Chrome on Ubuntu (for osmWebWizard)
sudo wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt --fix-broken install


export PYTHONPATH=$SUMO_HOME/tools
cd $PYTHONPATH
python3 osmWebWizard.py

# Run osmWebWizard, then use the system-recommended map size and tweak parameters as necessary.