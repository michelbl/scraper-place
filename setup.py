from setuptools import setup

setup(
    name='scraper-place',
    version='2.0.0',
    description='Scraper for https://www.marches-publics.gouv.fr/',
    url='https://github.com/michelbl/scraper-place',
    author='Michel Blancard',
    license='MIT',
    packages=['scraper_place'],
    install_requires=[
        'beautifulsoup4>=4.6.0',
        'boto3>=1.5.21',
        'elasticsearch>=6.1.1',
        'jupyter>=1.0.0',
        'matplotlib>=2.1.2',
        'paramiko>=2.4.0',
        'psycopg2>=2.7.3.2',
        'requests>=2.18.4',
        'Unidecode>=1.0.22',
    ],
    zip_safe=False,
)
