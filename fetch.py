import re

import requests
from bs4 import BeautifulSoup


url_search = 'https://www.marches-publics.gouv.fr/?page=entreprise.EntrepriseAdvancedSearch&AllCons'

page_state_regex = '<input type="hidden" name="PRADO_PAGESTATE" id="PRADO_PAGESTATE" value="([a-zA-Z0-9/+=]+)"'
link_regex = r'^https://www\.marches-publics\.gouv\.fr/\?page=entreprise\.EntrepriseDetailConsultation&refConsultation=([\d]+)&orgAcronyme=([\da-z]+)$'
reglement_regex = r'^index\.php\?page=entreprise\.EntrepriseDownloadReglement&reference=([a-zA-Z\d]+)&orgAcronyme=([\da-z]+)$'
boamp_regex = r'^http://www\.boamp\.fr/index\.php/avis/detail/([\d-]+)$'


def fetch_current_annonces():
    links_by_page = []
    page_state = None
    try:
        while(True):
            links, page_state = next_page(page_state)
            links_by_page.append(links)

    except NoMoreResultsException:
        pass
    
    all_links = []
    for links in links_by_page:
        all_links += links
    assert len(all_links) == len(set(all_links))
    
    return all_links


def fetch_data(link_annonce):
    annonce_id, org_acronym = re.match(link_regex, link_annonce).groups()
    url_annonce = 'https://www.marches-publics.gouv.fr/index.php?page=entreprise.EntrepriseDetailsConsultation&refConsultation={}&orgAcronyme={}'.format(annonce_id, org_acronym)
    
    r = requests.get(url_annonce)
    assert r.status_code == 200
    page_state = re.search(page_state_regex, r.text).groups()[0]
    
    
    # Get text data
    
    links_boamp = extract_links(r, boamp_regex)
    unique_boamp = list(set(links_boamp))
    links_boamp = unique_boamp

    soup = BeautifulSoup(r.text, 'html.parser')
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
        r = requests.post(url_annonce, data=data)
        assert r.status_code == 200

        content_type = r.headers['Content-Type']
        assert content_type in {'application/octet-stream', 'application/zip'}, content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_avis = re.match(regex_attachment, r.headers['Content-Disposition']).groups()[0]
        avis = r.content

    
    # Fetch reglement
    
    links_reglement = extract_links(r, reglement_regex)
    if len(links_reglement) == 0:
        filename_reglement = None
        reglement = None
        reglement_ref = None
    else:
        assert len(links_reglement) == 1
        link_reglement = links_reglement[0]
        reglement_ref = re.match(reglement_regex, link_reglement).groups()[0]
        url_reglement = 'https://www.marches-publics.gouv.fr/' + link_reglement
        r_reglement = requests.get(url_reglement)
        assert r.status_code == 200
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
        r = requests.post(url_annonce, data=data)
        assert r.status_code == 200

        content_type = r.headers['Content-Type']
        assert content_type in {'application/octet-stream', 'application/zip'}, content_type
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_complement = re.match(regex_attachment, r.headers['Content-Disposition']).groups()[0]
        complement = r.content

    
    # Get Dossier de Consultation aux Entreprises
    
    url_dce = 'https://www.marches-publics.gouv.fr/index.php?page=entreprise.EntrepriseDemandeTelechargementDce&refConsultation={}&orgAcronyme={}'.format(annonce_id, org_acronym)
    r = requests.get(url_dce)
    assert r.status_code == 200
    page_state = re.search(page_state_regex, r.text).groups()[0]
    
    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$validateButton',
        'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$RadioGroup': 'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$choixAnonyme',
    }
    r = requests.post(url_dce, data=data)
    assert r.status_code == 200
    page_state = re.search(page_state_regex, r.text).groups()[0]

    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload',
    }
    r = requests.post(url_dce, data=data)
    assert r.status_code == 200

    content_type = r.headers['Content-Type']
    assert content_type == 'application/zip', content_type
    regex_attachment = r'^attachment; filename="([^"]+)";$'
    filename_dce = re.match(regex_attachment, r.headers['Content-Disposition']).groups()[0]
    dce = r.content

    
    return annonce_id, org_acronym, links_boamp, reference, intitule, objet, reglement_ref, filename_reglement, reglement, filename_complement, complement, filename_avis, avis, filename_dce, dce
    
    
def init():
    # get page state
    r = requests.get(url_search)
    assert r.status_code == 200
    page_state = re.search(page_state_regex, r.text).groups()[0]

    # use page with 20 results
    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop',
        'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop': 20,
    }
    r = requests.post(url_search, data=data)
    assert r.status_code == 200
    links = extract_links(r, link_regex)
    page_state = re.search(page_state_regex, r.text).groups()[0]

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
    r = requests.post(url_search, data=data)
    
    if r.status_code == 500:
        raise NoMoreResultsException()
    
    assert r.status_code == 200
    links = extract_links(r, link_regex)
    page_state = re.search(page_state_regex, r.text).groups()[0]

    return links, page_state



def extract_links(request_result, regex):
    page = request_result.text
    soup = BeautifulSoup(page, 'html.parser')
    links = soup.find_all('a')
    hrefs = [link.attrs['href'] for link in links if 'href' in link.attrs]
    hrefs_clean = [href for href in hrefs if re.match(regex, href)]
    return hrefs_clean

