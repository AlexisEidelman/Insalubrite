# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 18:24:13 2016

@author: aeidelman
"""

import os
import pandas as pd

path = 'D:\data\SARAH\data'

# l'export depuis PostgreSQL créait des public.nom_de_la_table



def empyt_tables():
    empty_tables_liste = list()
    for file in os.listdir(path):
        tab = pd.read_csv(os.path.join(path, file), sep=';', nrows=10)
        if len(tab) == 0:
            empty_tables_liste += [file]
    return empty_tables_liste

voir = empyt_tables()