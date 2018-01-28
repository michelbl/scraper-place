from scraper_place import fetch, glacier, content_indexing

fetch.fetch_new_dce()
glacier.save()
content_indexing.index()
