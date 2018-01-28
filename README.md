# `scaper-place`

`scraper-place` est un scraper pour le site internet gouvernemental https://www.marches-publics.gouv.fr/ (PLACE pour PLateforme d'AChat de l'Etat). `scraper-place` n'a rien à voir avec Jean-Vincent Placé.

PLACE permet d'accéder aux [Dossiers de Consultation des Entreprises (DCE)](https://fr.wikipedia.org/wiki/Dossier_de_consultation_des_entreprises) pour les consultations en cours. Malheureusement, PLACE ne permet pas un accès pratique aux ressources publiées pour tous les usages. D'une part, les dossiers des publications passées sont retirées. D'autre part, le fonctionnement du site rend malaisée le requetage automatisé des DCE.

`scaper-place` va récupérer tous les DCE (ainsi que les métadonnées associées) pour les répliquer localement et les indexer.

Le droit d'accès applicable aux DCE est résumé dans une [fiche de la CADA](http://www.cada.fr/marches-publics,6085.html).


## How PLACE works, how to parse it

PLACE uses the PHP framework PRADO. It stores the current navigation state in the `PRADO_PAGESTATE`, an encoded variable of about 100kB. This variable is required (along with `PRADO_POSTBACK_TARGET` and optional parameters) to perform a request. There is no way to get a list of all the available DCE. The search engine is clearly not an option. The most convenient way I found is to request the paginated list of all the current consultations, set the pagination number to 20 (the maximum) and request all the pages in order. Naturally, given the id of a consultation, three successive requests are needed to access the document.

Curiously, a small fraction of the DCE appear in several pages, and this is not related to the addition of documents during the course of the parsing. I guess such a feature would be very difficult to implement purposefully.


## Install `scaper-place`

### Prerequisites

* Install `postgresql`>=9.0 (may work on prior versions). Make sure it uses UTF-8 encoding.
* If you plan to replicate the files on AWS Glacier, create a vault and create a IAM user with upload permission.
* If you plan to index the data with ElasticSearch, install it. Make sure a tika server 1.17 (older versions may work) is reachable with the options `-enableUnsecureFeatures` and `-enableFileUrl`.
* Create a python virtual env with python>=3.5 (may work on previous versions). I suggest using `pew`.

### Installation

* Clone this repository.
* In the repository directory: `pip install --editable .`
* Copy `config.ini.example` to `config.ini` and set your configuration.
* Create the directory you configured in `config.ini` and make sure it is writable by the process that will run `scraper-place`.
* Create a new database user with all privileges on a new table, with access by password (`md5` in `pg_hda.conf`).
* Run `create_tables.ipynb` to prepare the database and ElasticSearch.


## Usage

```
from scraper_place import fetch, glacier, content_indexing

fetch.fetch_new_dce()
glacier.save()
content_indexing.index()
```

* `fetch.fetch_new_dce()` parses https://www.marches-publics.gouv.fr/ and fetches new DCEs.
* `glacier.save()` sends a copy to AWS Glacier
* `content_indexing.index()` extracts content with Apache Tika and feeds it to ElasticSearch
