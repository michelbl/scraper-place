"""fetch: Fetch the new DCE from https://www.marches-publics.gouv.fr/

Use fetch_new_dce() to store metadata in database and store the archives in the public directory.
Use fetch_current_annonces() to fetch the list of currently available DCE.
Use fetch_data() to fetch the metadata and the files custituting a DCE.
"""

import datetime
import re
from collections import Counter
import traceback
import os

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

from scraper_place.config import CONFIG_ENV, STATE_FETCH_OK, build_internal_filepath


URL_SEARCH = 'https://www.marches-publics.gouv.fr/?page=entreprise.EntrepriseAdvancedSearch&AllCons'

PAGE_STATE_REGEX = 'name="PRADO_PAGESTATE" id="PRADO_PAGESTATE" value="([a-zA-Z0-9/+=]+)"'
LINK_REGEX = r'^https://www\.marches-publics\.gouv\.fr/app\.php/entreprise/consultation/([\d]+)\?orgAcronyme=([\da-z]+)$'
REGLEMENT_REGEX = r'^/index.php\?page=entreprise\.EntrepriseDownloadReglement&reference=([a-zA-Z\d]+)&orgAcronyme=([\da-z]+)$'
BOAMP_REGEX = r'^http://www\.boamp\.fr/(?:index\.php/)?avis/detail/([\d-]+)(?:/[\d]+)?$'



def fetch_new_dce():
    """fetch_new_dce: fetch the DCEs that are not already in the database, stores metadata in database and stores the archives in the public directory.
    """

    if CONFIG_ENV['env'] == 'production':
        nb_pages = 0
    else:
        nb_pages = 1

    links = fetch_current_annonces(nb_pages=nb_pages)

    nb_processed = 0
    for link in links:
        nb_processed += process_link(link)
    print("Info: Processed {} DCE".format(nb_processed))


def process_link(link):
    """
    process_link : Download data and store it in database.
    Return the number of stored DCE (0 or 1).
    """
    annonce_id = re.match(LINK_REGEX, link).groups()[0]

    client = MongoClient()
    collection = client.place.dce

    # abort if the DCE is already processed
    if collection.count_documents({'annonce_id': annonce_id}, limit=1):
        return 0

    try:
        annonce_data = fetch_data(link)
    except Exception as exception:
        print("Warning: exception occured ({}: {}) on {}".format(type(exception).__name__, exception, link))
        traceback.print_exc()
        return 0

    annonce_data['fetch_datetime'] = datetime.datetime.now()
    annonce_data['state'] = STATE_FETCH_OK

    collection.insert_one(annonce_data)
    client.close()

    return 1


def fetch_current_annonces(nb_pages=0):
    """fetch_current_annonces(): Fetch the list of currently available DCE.

    nb_pages: number of pages to fetch, 0 to set no limit (for example, you can set to 1 for a development setup)

    Returns a list of URL.
    """
    links_by_page = []
    page_state = None
    cookie = None
    try:
        counter = 0
        while (nb_pages == 0) or (counter < nb_pages):
            links, page_state, cookie = next_page(page_state, cookie)
            links_by_page.append(links)
            counter += 1

    except NoMoreResultsException:
        pass

    all_links = []
    for links in links_by_page:
        all_links += links
    if len(all_links) != len(set(all_links)):
        duplicates = [k for k, v in Counter(all_links).items() if v > 1]
        nb_duplicates = len(duplicates)
        print('Warning: {} DCE found multiple times'.format(nb_duplicates))

    return all_links


def fetch_data(link_annonce):
    """fetch_data(): Fetch the metadata and the files of a given DCE.
    """

    annonce_id, org_acronym = re.match(LINK_REGEX, link_annonce).groups()
    url_annonce = 'https://www.marches-publics.gouv.fr/index.php?page=entreprise.EntrepriseDetailsConsultation&refConsultation={}&orgAcronyme={}'.format(annonce_id, org_acronym)

    response = requests.get(url_annonce)
    assert response.status_code == 200
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]


    # Get text data

    links_boamp = extract_links(response, BOAMP_REGEX)
    unique_boamp = list(set(links_boamp))
    links_boamp = unique_boamp

    soup = BeautifulSoup(response.text, 'html.parser')

    recap_data = soup.find_all(class_="col-md-10 text-justify")

    assert recap_data[0].find('label').text.strip() == "Référence :"
    reference = recap_data[0].find('div').text.strip()

    assert recap_data[1].find('label').text.strip() == "Intitulé :"
    intitule = recap_data[1].find('div').text.strip()

    assert recap_data[2].find('label').text.strip() == "Objet :"
    objet = recap_data[2].find('div').text.strip()


    # Get links to files

    publicite_tabs = soup.find_all(id='pub')
    assert len(publicite_tabs) == 1
    publicite_tab = publicite_tabs[0]
    file_links = publicite_tab.find_all('a')

    links_reglements = []
    links_dces = []
    links_avis = []
    links_complements = []

    for link in file_links:
        link_href = link.attrs['href']

        if re.match(BOAMP_REGEX, link_href):
            continue
        if not link_href:
            continue

        if 'id' not in link.attrs:
            # "liens directs"
            continue

        link_id = link.attrs['id']

        if link_id == 'linkDownloadReglement':
            links_reglements.append(link_href)
        elif link_id == 'linkDownloadDce':
            links_dces.append(link_href)
        elif link_id == 'linkDownloadAvis':
            links_avis.append(link_href)
        elif link_id == 'linkDownloadComplement':
            links_complements.append(link_href)
        elif link_id == 'linkDownloadDume':
            pass  # "DUME acheteur" does not contain useful information
        else:
            raise Exception('Unknown link type {} : {}'.format(link_id, link_href))

    assert len(links_reglements) <= 1
    link_reglement = links_reglements[0] if links_reglements else None
    assert len(links_dces) <= 1
    link_dce = links_dces[0] if links_dces else None
    # Avis rectificatifs...
    # assert len(links_avis) <= 1
    link_avis = links_avis[0] if links_avis else None
    assert len(links_complements) <= 1
    link_complement = links_complements[0] if links_complements else None


    def write_response_to_file(annonce_id, filename, file_type, response):
        internal_filepath = build_internal_filepath(annonce_id=annonce_id, original_filename=filename, file_type=file_type)
        with open(internal_filepath, 'wb') as file_object:
            for chunk in response.iter_content(8192):
                file_object.write(chunk)
        return os.path.getsize(internal_filepath)


    # Get avis

    filename_avis = None
    file_size_avis = None
    if link_avis:
        response_avis = requests.get('https://www.marches-publics.gouv.fr{}'.format(link_avis), stream=True)
        assert response_avis.status_code == 200
        regex_attachment = r'^attachment; filename="([^"]+)"'
        filename_avis = re.match(regex_attachment, response_avis.headers['Content-Disposition']).groups()[0]

        file_size_avis = write_response_to_file(annonce_id=annonce_id, filename=filename_avis, file_type='avis', response=response_avis)


    # Fetch reglement

    filename_reglement = None
    reglement_ref = None
    file_size_reglement = None
    if link_reglement:
        reglement_ref = re.match(REGLEMENT_REGEX, link_reglement).groups()[0]
        response_reglement = requests.get('https://www.marches-publics.gouv.fr{}'.format(link_reglement), stream=True)
        assert response_reglement.status_code == 200
        content_type = response_reglement.headers['Content-Type']
        assert content_type in {'application/octet-stream', 'application/zip'}, content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_reglement = re.match(regex_attachment, response_reglement.headers['Content-Disposition']).groups()[0]

        file_size_reglement = write_response_to_file(annonce_id=annonce_id, filename=filename_reglement, file_type='reglement', response=response_reglement)


    # Fetch complement

    filename_complement = None
    file_size_complement = None
    if link_complement:
        response_complement = requests.get('https://www.marches-publics.gouv.fr{}'.format(link_complement), stream=True)
        assert response_complement.status_code == 200
        regex_attachment = r'^attachment; filename="([^"]+)"'
        filename_complement = re.match(regex_attachment, response_complement.headers['Content-Disposition']).groups()[0]

        file_size_complement = write_response_to_file(annonce_id=annonce_id, filename=filename_complement, file_type='complement', response=response_complement)


    # Get Dossier de Consultation aux Entreprises

    filename_dce = None
    file_size_dce = None
    if link_dce:
        url_dce = 'https://www.marches-publics.gouv.fr/index.php?page=entreprise.EntrepriseDemandeTelechargementDce&refConsultation={}&orgAcronyme={}'.format(annonce_id, org_acronym)
        response_dce = requests.get(url_dce)
        assert response_dce.status_code == 200
        page_state = re.search(PAGE_STATE_REGEX, response_dce.text).groups()[0]
        cookie = response_dce.headers['Set-Cookie']

        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$validateButton',
            'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$RadioGroup': 'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$choixAnonyme',
        }
        response_dce2 = requests.post(url_dce, headers={'Cookie': cookie}, data=data)
        assert response_dce2.status_code == 200
        page_state = re.search(PAGE_STATE_REGEX, response_dce2.text).groups()[0]

        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload',
        }
        response_dce3 = requests.post(url_dce, headers={'Cookie': cookie}, data=data, stream=True)
        assert response_dce3.status_code == 200

        content_type = response_dce3.headers['Content-Type']
        assert content_type == 'application/zip', content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_dce = re.match(regex_attachment, response_dce3.headers['Content-Disposition']).groups()[0]

        file_size_dce = write_response_to_file(annonce_id=annonce_id, filename=filename_dce, file_type='dce', response=response_dce3)


    return {
        'annonce_id': annonce_id,
        'org_acronym': org_acronym,
        'links_boamp': links_boamp,
        'reference': reference,
        'intitule': intitule,
        'objet': objet,
        'reglement_ref': reglement_ref,
        'filename_reglement': filename_reglement,
        'filename_complement': filename_complement,
        'filename_avis': filename_avis,
        'filename_dce': filename_dce,
        'file_size_reglement': file_size_reglement,
        'file_size_complement': file_size_complement,
        'file_size_avis': file_size_avis,
        'file_size_dce': file_size_dce,
    }



def init():
    """init(): Fetch the first page of the row.
    """

    # get page state
    response = requests.get(URL_SEARCH)
    assert response.status_code == 200, response.status_code
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]
    cookie = response.headers['Set-Cookie']

    # use page with 20 results
    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop',
        'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop': 20,
    }
    response = requests.post(URL_SEARCH, headers={'Cookie': cookie}, data=data)
    assert response.status_code == 200, response.status_code
    links = extract_links(response, LINK_REGEX)
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

    return links, page_state, cookie

class NoMoreResultsException(Exception):
    pass

def next_page(page_state, cookie):
    if not page_state:
        return init()

    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$PagerTop$ctl2',
    }
    response = requests.post(URL_SEARCH, headers={'Cookie': cookie}, data=data)

    if response.status_code == 500:
        raise NoMoreResultsException()

    assert response.status_code == 200
    links = extract_links(response, LINK_REGEX)
    page_state_new = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

    if page_state == page_state_new:
        raise NoMoreResultsException()

    return links, page_state_new, cookie



def extract_links(request_result, regex):
    page = request_result.text
    soup = BeautifulSoup(page, 'html.parser')
    links = soup.find_all('a')
    hrefs = [link.attrs['href'] for link in links if 'href' in link.attrs]
    hrefs_clean = [href for href in hrefs if re.match(regex, href)]
    return hrefs_clean
