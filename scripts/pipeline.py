from scraper_place import fetch, glacier, extraction, indexation

fetch.fetch_new_dce()
glacier.save()
extraction.extract()
indexation.index()
