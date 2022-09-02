from setuptools import setup

setup(
    name='scraper-place',
    version='2.2.0',
    description='Scraper for https://www.marches-publics.gouv.fr/',
    url='https://github.com/michelbl/scraper-place',
    author='Michel Blancard',
    license='MIT',
    packages=['scraper_place'],
    install_requires=[
        'awscli>=1.25.67',
        'beautifulsoup4>=4.11.1',
        'boto3>=1.24.66',
        'elasticsearch>=8.4.0',
        'jupyter>=1.0.0',
        'matplotlib>=2.1.2',
        'pymongo>=4.2.0',
        'requests>=2.28.1',
        'Unidecode>=1.3.4',
    ],
    zip_safe=False,
)
