#!/usr/bin/env bash
#set -o xtrace
set -o errexit

PYTHON_PATH=/home/debian/.local/share/virtualenvs/place/bin/python
SCRAPER_PLACE_PATH=/srv/scraper-place/scraper_place
$PYTHON_PATH $SCRAPER_PLACE_PATH/fetch.py
$PYTHON_PATH $SCRAPER_PLACE_PATH/glacier.py
touch /srv/scraper_place/maintenance.lock
sudo systemctl stop betterplace.service
sudo systemctl stop elasticsearch.service
sudo systemctl start tika.service
sleep 60
$PYTHON_PATH $SCRAPER_PLACE_PATH/extraction.py
sudo systemctl stop tika.service
sudo systemctl start elasticsearch.service
sleep 60
$PYTHON_PATH $SCRAPER_PLACE_PATH/indexation.py
sudo systemctl start betterplace.service
rm /srv/scraper_place/maintenance.lock
