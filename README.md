# `scaper-place`

`scraper-place` est un scraper pour le site internet gouvernemental https://www.marches-publics.gouv.fr/ (PLACE pour PLateforme d'AChat de l'Etat). `scraper-place` n'a rien à voir avec Jean-Vincent Placé.

PLACE permet d'accéder aux [Dossiers de Consultation des Entreprises (DCE)](https://fr.wikipedia.org/wiki/Dossier_de_consultation_des_entreprises) pour les consultations en cours. Malheureusement, PLACE ne permet pas un accès pratique aux ressources publiées pour tous les usages. D'une part, les dossiers des publications passées sont retirées. D'autre part, le fonctionnement du site rend malaisée le requetage automatisé des DCE.

`scaper-place` va récupérer tous les DCE (ainsi que les métadonnées associées) pour les répliquer localement et les indexer.

Le droit d'accès applicable aux DCE est résumé dans une [fiche de la CADA](http://www.cada.fr/marches-publics,6085.html).


## How PLACE works, how to parse it

PLACE uses the PHP framework PRADO. It stores the current navigation state in the `PRADO_PAGESTATE`, an encoded variable of about 100kB. This variable is required (along with `PRADO_POSTBACK_TARGET` and optional parameters) to perform a request. There is no way to get a list of all the available DCE. The search engine is clearly not an option. The most convenient way I found is to request the paginated list of all the current consultations, set the pagination number to 20 (the maximum) and request all the pages in order. Naturally, given the id of a consultation, three successive requests are needed to access the document.

Curiously, a small fraction of the DCE appear in several pages, and this is not related to the addition of documents during the course of the parsing. I guess such a feature would be very difficult to implement purposefully.


## Features

* scrap PLACE every night
* text extraction using Apache Tika
* indexation with ElasticSearch
* backup of both documents and metadata on AWS S3

## Install `scraper-place`

### Prerequisites

* Install mongodb 6 (other versions may work).
* If you plan to replicate the files on AWS Glacier, create a vault and create a IAM user with upload permission.
* If you plan to index the data with ElasticSearch, install it.
* Create a python virtual env with python>=3.9 (I suggest using `pew`).

### Installation

* Clone this repository.
* In the repository directory: `pip install --editable .`
* Copy `config.ini.example` to `config.ini` and set your configuration.
* Create the directories you configured in `config.ini` and make sure they are writable by the process that will run `scraper-place`.
* Import metadata to mongo (see `scripts/import-to-mongo.ipynb`)
* Configure ElasticSearch (see `elasticsearch.yml`)
* Set up the ElasticSearch index (see `scripts/create_index.ipynb`)
* Setup services (see `betterplace.service`, `tika.service`)
* Configure nginx (see `betterplace.info`)
* Setup crons to trigger `scripts/nightly_scraping.sh` and `scripts/backup_metadata.py` (see `crontab` for an example)

## Misc

To use debug logging on elasticsearch:

```
curl -XPUT 'localhost:9200/_cluster/settings' --data '{"transient":{"logger._root":"DEBUG"}}' -H'Content-Type: application/json'
```
