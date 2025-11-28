#!/bin/sh

<< COMMENTOUT
# For Convienience, add the $USER, $osmWebWizard_RESULT, $GTFS_Static, $SUMO_HOME directory to the .bashrc file.
COMMENTOUT

cd $osmWebWizard_RESULT
## Select the directory which contains osm.net.xml.gz, osm_ptlines.xml

echo $GTFS_Static
## Select the directory which contains *.zip files of GTFS Static data.

cd ~
python3 -m venv .venv 
source .venv/bin/activate

cd $SUMO_HOME/tools
pip3 install --upgrade pip
pip3 install -r requirements.txt

sudo chown -R $USER:$USER $SUMO_HOME

cd $SUMO_HOME
# Routes from OSM
python3 tools/import/gtfs/gtfs2pt.py -n $osmWebWizard_RESULT/Toyama-Station_wihtoutPT/osm.net.xml.gz \
 --gtfs $GTFS_Static/gtfs_static_20251007_194230.zip --date 20251007 --modes bus \
 --osm-routes $osmWebWizard_RESULT/Toyama-Station_wihtoutPT/osm_ptlines.xml --repair

# Run the simulation with sumo-gui to visualize the result
sumo-gui -n $osmWebWizard_RESULT/Toyama-Station_wihtoutPT/osm.net.xml.gz \
 --additional vtypes.xml,gtfs_pt_stops.add.xml,gtfs_pt_vehicles.add.xml


# # Routes from shortest path
# python3 tools/import/gtfs/gtfs2pt.py -n $osmWebWizard_RESULT/Minami-Toyama/osm.net.xml.gz \
#  --gtfs $GTFS_Static/gtfs_static_20251007_194230.zip --date 20251007 --modes bus

# # echo '2025.11.25. # Touble shooting for "No GTFS data found for the given modes tram."'
# grep -n "route_type" 
# # vim route.txt
# cd $GTFS_Static
# wget -O gtfs_VBB.zip https://unternehmen.vbb.de/fileadmin/user_upload/VBB/Dokumente/API-Datensaetze/gtfs-mastscharf/GTFS.zip
# unzip gtfs_VBB.zip -d gtfs_VBB
# cd gtfs_VBB
# grep -n "route_type" routes.txt
# vim routes.txt










# deactivate