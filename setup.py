# -*- coding: utf-8 -*-
"""

"""

from setuptools import setup, find_packages

import insalubrite

setup(
    name = 'insalubrite',
    version = '0.1.0.dev',
    url = 'https://github.com/AlexisEidelman/Insalubrite.git',
    license = 'http://www.fsf.org/licensing/licenses/agpl-3.0.html',
    author='Alexis Eidelman',
    description='Répertoire pour déterminer les bâtiments insalubres.',
    #long_description=__doc__,
    #py_modules=['insalubrite'],
    packages=find_packages(), #['Apur', 'Sarah'],
    zip_safe=False,
    platforms='any',
    install_requires=['pandas'],
    include_package_data=True,
)

