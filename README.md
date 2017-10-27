# `scaper-place`

`scraper-place` est un scraper pour le site internet gouvernemental https://www.marches-publics.gouv.fr/ (PLACE pour PLateforme d'AChat de l'Etat). `scraper-place` n'a rien à voir avec Jean-Vincent Placé.

PLACE permet d'accéder aux [Dossiers de Consultation des Entreprises (DCE)](https://fr.wikipedia.org/wiki/Dossier_de_consultation_des_entreprises) pour les consultations en cours. Malheureusement, PLACE ne permet pas un accès pratique aux ressources publiées pour tous les usages. D'une part, les dossiers des publications passées sont retirées. D'autre part, le fonctionnement du site rend malaisée le requetage automatisé des DCE.

`scaper-place` va récupérer tous les DCE (ainsi que les métadonnées associées) pour les répliquer localement.

Le droit d'accès applicable aux DCE est résumé dans une [fiche de la CADA](http://www.cada.fr/marches-publics,6085.html).


## How PLACE works, how to parse it

PLACE uses the PHP framework PRADO. It stores the current navigation state in the `PRADO_PAGESTATE`, an encoded variable of about 100kB. This variable is required (along with `PRADO_POSTBACK_TARGET` and optional parameters) to perform a request. There is no way to get a list of all the available DCE. The search engine is clearly not an option. The most convenient way I found is to request the paginated list of all the current consultations, set the pagination number to 20 (the maximum) and request all the pages in order. Naturally, given the id of a consultation, three successive requests are needed to access the document.

Curiously, a small fraction of the DCE appear in several pages, and this is not related to the addition of documents during the course of the parsing. I guess such a feature would be very difficult to implement purposefully.


## Install `scaper-place`

* Install `postgresql`, a new python virtual env with python3 (I suggest using `pew`)
* `pip install -r requirements.txt`
* Copy `config.ini.example` to `config.ini` and set your configuration.
* Create the directories you configured in `config.ini`, except the temporary directory.
* Create a new database user with all privileges on a new table, with access by password (`md5` in `pg_hda.conf`).
* Run `create_tables.ipynb` to prepare the database.


## Usage

* Run `update_tables.py` to scrap PLACE and update the local table `dce`. The files are stored in `dir` (see `config.ini`). You can set the optional parameter `nb_pages` to 1 in the call of `fetch.fetch_current_annonces()` for a developpement setup.
* Run `copy_files.py` to unzip archives, update the table `files` and copy the files :
  * original archive and unzipped files are copied to `target_dir` when the unzip operation is successful
  * a copy of the original file is stored in `archive_dir` when the unzip operation is successful 
  * a copy of the original file is stored in `badfiles_dir` when the unzip operation is unsuccessful
  * the original archive in `dir` is removed
