#!/usr/bin/env bash
#set -o xtrace
set -o errexit

LOCK_FILE=/srv/scraper-place/nightly.lock
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  printf '%s WARNING: nightly scraping already running, not starting another\n' "$(date '+%Y-%m-%d %H:%M:%S')" >> /var/log/scraper-place/scraper-place.log
  exit 0
fi

PYTHON_PATH=/srv/scraper-place/.venv/bin/python
SCRAPER_PLACE_PATH=/srv/scraper-place/scraper_place
$PYTHON_PATH $SCRAPER_PLACE_PATH/fetch.py
$PYTHON_PATH $SCRAPER_PLACE_PATH/glacier.py
touch /srv/scraper-place/maintenance.lock
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
rm /srv/scraper-place/maintenance.lock
