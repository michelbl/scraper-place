"""fetch: Fetch the new DCE from https://www.marches-publics.gouv.fr/

Use fetch_new_dce() to store metadata in database and store the archives in the public directory.
Use fetch_current_annonces() to fetch the list of currently available DCE.
Use fetch_data() to fetch the metadata and the files custituting a DCE.
"""

import datetime
import re
from collections import Counter
import traceback

import requests
from bs4 import BeautifulSoup
import psycopg2

from scraper_place.config import CONFIG_DATABASE, CONFIG_ENV, STATE_FETCH_OK, build_internal_filepath


URL_SEARCH = 'https://www.marches-publics.gouv.fr/?page=entreprise.EntrepriseAdvancedSearch&AllCons'

PAGE_STATE_REGEX = '<input type="hidden" name="PRADO_PAGESTATE" id="PRADO_PAGESTATE" value="([a-zA-Z0-9/+=]+)"'
LINK_REGEX = r'^https://www\.marches-publics\.gouv\.fr/\?page=entreprise\.EntrepriseDetailConsultation&refConsultation=([\d]+)&orgAcronyme=([\da-z]+)$'
REGLEMENT_REGEX = r'^index\.php\?page=entreprise\.EntrepriseDownloadReglement&reference=([a-zA-Z\d]+)&orgAcronyme=([\da-z]+)$'
BOAMP_REGEX = r'^http://www\.boamp\.fr/index\.php/avis/detail/([\d-]+)$'




def fetch_new_dce():
    """fetch_new_dce: fetch the DCEs that are not already in the database, stores metadata in database and stores the archives in the public directory.
    """

    # Open connection
    connection = psycopg2.connect(
        dbname=CONFIG_DATABASE['name'],
        user=CONFIG_DATABASE['username'],
        password=CONFIG_DATABASE['password'],
    )
    cursor = connection.cursor()

    if CONFIG_ENV['env'] == 'production':
        nb_pages = 0
    else:
        nb_pages = 1

    links = fetch_current_annonces(nb_pages=nb_pages)

    nb_processed = 0
    for link in links:
        nb_processed += process_link(link, connection, cursor)
    print("Info: Processed {} DCE".format(nb_processed))

    cursor.close()
    connection.close()


def process_link(link, connection, cursor):
    """
    process_link : Download data and store it in database.
    Return the number of stored DCE (0 or 1).
    """
    annonce_id, org_acronym = re.match(LINK_REGEX, link).groups()

    # abort if the DCE is already processed
    cursor.execute("SELECT annonce_id, org_acronym FROM dce WHERE annonce_id = %s AND org_acronym = %s;", (annonce_id, org_acronym))
    results = cursor.fetchall()
    if results:
        return 0

    try:
        (
            annonce_id, org_acronym, links_boamp, reference, intitule, objet, reglement_ref,
            filename_reglement, reglement,
            filename_complement, complement,
            filename_avis, avis,
            filename_dce, dce
        ) = fetch_data(link)
    except Exception as exception:
        print("Warning: exception occured ({}: {}) on {}".format(type(exception).__name__, exception, link))
        traceback.print_exc()
        return 0

    now = datetime.datetime.now()

    file_types = ['reglement', 'complement', 'avis', 'dce']
    filenames = [filename_reglement, filename_complement, filename_avis, filename_dce]
    file_contents = [reglement, complement, avis, dce]
    for file_type, filename, file_content in zip(file_types, filenames, file_contents):
        if file_content:
            internal_filepath = build_internal_filepath(annonce_id, org_acronym, filename, file_type)
            with open(internal_filepath, 'wb') as file_object:
                file_object.write(file_content)

    cursor.execute(
        """
        INSERT INTO dce (
            annonce_id, org_acronym, links_boamp,
            reference, intitule, objet,
            reglement_ref, filename_reglement, filename_complement, filename_avis, filename_dce,
            fetch_datetime,
            state
            )
            VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s,
            %s
           )""",
        (
            annonce_id, org_acronym, links_boamp,
            reference, intitule, objet,
            reglement_ref, filename_reglement, filename_complement, filename_avis, filename_dce,
            now,
            STATE_FETCH_OK
            )
        )
    connection.commit()
    return 1


def fetch_current_annonces(nb_pages=0):
    """fetch_current_annonces(): Fetch the list of currently available DCE.

    nb_pages: number of pages to fetch, 0 to set no limit (for example, you can set to 1 for a developpement setup)

    Returns a list of URL.
    """
    links_by_page = []
    page_state = None
    try:
        counter = 0
        while (nb_pages == 0) or (counter < nb_pages):
            links, page_state = next_page(page_state)
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
    reference = soup.find(id="ctl0_CONTENU_PAGE_idEntrepriseConsultationSummary_reference").string
    intitule = soup.find(id="ctl0_CONTENU_PAGE_idEntrepriseConsultationSummary_intitule").string
    objet = soup.find(id="ctl0_CONTENU_PAGE_idEntrepriseConsultationSummary_objet").string


    # Get avis

    link_avis = soup.find(id="ctl0_CONTENU_PAGE_repeaterAvis_ctl1_linkDownloadAvis")
    if link_avis.parent.parent.attrs['style'] == 'display:none':
        filename_avis = None
        avis = None
    else:
        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$repeaterAvis$ctl1$linkDownloadAvis',
        }
        response = requests.post(url_annonce, data=data)
        assert response.status_code == 200

        content_type = response.headers['Content-Type']
        assert content_type in {'application/octet-stream', 'application/zip'}, content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_avis = re.match(regex_attachment, response.headers['Content-Disposition']).groups()[0]
        avis = response.content


    # Fetch reglement

    links_reglement = extract_links(response, REGLEMENT_REGEX)
    if not links_reglement:
        filename_reglement = None
        reglement = None
        reglement_ref = None
    else:
        assert len(links_reglement) == 1
        link_reglement = links_reglement[0]
        reglement_ref = re.match(REGLEMENT_REGEX, link_reglement).groups()[0]
        url_reglement = 'https://www.marches-publics.gouv.fr/' + link_reglement
        r_reglement = requests.get(url_reglement)
        assert response.status_code == 200
        content_type = r_reglement.headers['Content-Type']
        assert content_type in {'application/octet-stream', 'application/zip'}, content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_reglement = re.match(regex_attachment, r_reglement.headers['Content-Disposition']).groups()[0]
        reglement = r_reglement.content


    # Fetch complement

    link_complement = soup.find(id="ctl0_CONTENU_PAGE_linkDownloadComplement")
    if link_complement.parent.attrs['style'] == 'display:none':
        filename_complement = None
        complement = None
    else:
        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$linkDownloadComplement',
        }
        response = requests.post(url_annonce, data=data)
        assert response.status_code == 200

        content_type = response.headers['Content-Type']
        assert content_type in {'application/octet-stream', 'application/zip'}, content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_complement = re.match(regex_attachment, response.headers['Content-Disposition']).groups()[0]
        complement = response.content


    # Get Dossier de Consultation aux Entreprises

    link_dce = soup.find(id="ctl0_CONTENU_PAGE_linkDownloadDce")
    if link_dce.parent.parent.attrs['style'] == 'display:none':
        filename_dce = None
        dce = None
    else:
        url_dce = 'https://www.marches-publics.gouv.fr/index.php?page=entreprise.EntrepriseDemandeTelechargementDce&refConsultation={}&orgAcronyme={}'.format(annonce_id, org_acronym)
        response = requests.get(url_dce)
        assert response.status_code == 200
        page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$validateButton',
            'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$RadioGroup': 'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$choixAnonyme',
        }
        response = requests.post(url_dce, data=data)
        assert response.status_code == 200
        page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload',
        }
        response = requests.post(url_dce, data=data)
        assert response.status_code == 200

        content_type = response.headers['Content-Type']
        assert content_type == 'application/zip', content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_dce = re.match(regex_attachment, response.headers['Content-Disposition']).groups()[0]
        dce = response.content


    return annonce_id, org_acronym, links_boamp, reference, intitule, objet, reglement_ref, filename_reglement, reglement, filename_complement, complement, filename_avis, avis, filename_dce, dce


def init():
    """init(): Fetch the first page of the row.
    """

    # get page state
    response = requests.get(URL_SEARCH)
    assert response.status_code == 200
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

    # use page with 20 results
    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop',
        'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop': 20,
    }
    response = requests.post(URL_SEARCH, data=data)
    assert response.status_code == 200
    links = extract_links(response, LINK_REGEX)
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

    return links, page_state

class NoMoreResultsException(Exception):
    pass

def next_page(page_state):
    if not page_state:
        return init()

    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$PagerTop$ctl2',
    }
    response = requests.post(URL_SEARCH, data=data)

    if response.status_code == 500:
        raise NoMoreResultsException()

    assert response.status_code == 200
    links = extract_links(response, LINK_REGEX)
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

    return links, page_state



def extract_links(request_result, regex):
    page = request_result.text
    soup = BeautifulSoup(page, 'html.parser')
    links = soup.find_all('a')
    hrefs = [link.attrs['href'] for link in links if 'href' in link.attrs]
    hrefs_clean = [href for href in hrefs if re.match(regex, href)]
    return hrefs_clean
