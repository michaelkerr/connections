try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

config = {
    'name': 'connections',
    'version': '0.1',
    'packages': find_packages[],
    'install_requires': ['nose', 'datetime', 'json', 'os', 'elasticsearch'],
    'description': 'package pulls connections out of IO data',
    'author': 'Michael Kerr',
    'author_email': 'mkerr09@gmail.com',
    'url': 'URL to get it at.',
    'download_url': 'Where to download it.',
    'scripts': [],
    include_package_data = True
}

setup(**config)
