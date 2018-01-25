from setuptools import setup

setup(
    name='scraper-place',
    version='1.0.0',
    description='Scraper for https://www.marches-publics.gouv.fr/',
    url='https://github.com/michelbl/scraper-place',
    author='Michel Blancard',
    license='MIT',
    packages=['scraper_place'],
    install_requires=[
        'beautifulsoup4>=4.6.0',
        'jupyter>=1.0.0',
        'psycopg2>=2.7.3.2',
        'requests>=2.18.4',
    ],
    zip_safe=False
)
