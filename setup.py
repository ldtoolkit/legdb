# -*- coding: utf-8 -*-
from setuptools import setup

packages = \
['legdb']

package_data = \
{'': ['*']}

setup_kwargs = {
    'name': 'legdb',
    'version': '0.1.0',
    'description': 'Lightning Embedded GraphDB',
    'long_description': None,
    'author': 'Roman Inflianskas',
    'author_email': 'infroma@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'python_requires': '>=3.6,<4.0',
}


setup(**setup_kwargs)
